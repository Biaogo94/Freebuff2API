package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RunManager struct {
	cfg    Config
	logger *log.Logger
	pools  []*tokenPool
	next   atomic.Uint64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type tokenPool struct {
	name   string
	token  string
	cfg    Config
	client *UpstreamClient
	logger *log.Logger

	mu            sync.Mutex
	runs          map[string]*managedRun // agentID → current run
	draining      []*managedRun
	lastError     string
	cooldownUntil time.Time
	sessionStatus string
	sessionID     string
	sessionPollAt time.Time
	sessionExpiry time.Time
}

type managedRun struct {
	id           string
	agentID      string
	startedAt    time.Time
	inflight     int
	requestCount int
	finishing    bool
}

type runLease struct {
	pool               *tokenPool
	run                *managedRun
	freebuffInstanceID string
}

type tokenSnapshot struct {
	Name          string        `json:"name"`
	Runs          []runSnapshot `json:"runs"`
	DrainingRuns  int           `json:"draining_runs"`
	CooldownUntil time.Time    `json:"cooldown_until,omitempty"`
	LastError     string        `json:"last_error,omitempty"`
}

type runSnapshot struct {
	AgentID      string    `json:"agent_id"`
	RunID        string    `json:"run_id"`
	StartedAt    time.Time `json:"started_at"`
	Inflight     int       `json:"inflight"`
	RequestCount int       `json:"request_count"`
}

func NewRunManager(cfg Config, client *UpstreamClient, logger *log.Logger) *RunManager {
	pools := make([]*tokenPool, 0, len(cfg.AuthTokens))
	for index, token := range cfg.AuthTokens {
		pools = append(pools, &tokenPool{
			name:   fmt.Sprintf("token-%d", index+1),
			token:  token,
			cfg:    cfg,
			client: client,
			runs:   make(map[string]*managedRun),
			logger: logger,
		})
	}

	return &RunManager{
		cfg:    cfg,
		logger: logger,
		pools:  pools,
		stopCh: make(chan struct{}),
	}
}

func (m *RunManager) Start(ctx context.Context, agentIDs []string) {
	// Pre-warm runs for all free agents in background.
	// The server is already listening; if a request arrives before
	// pre-warming finishes, acquire() will lazily create the run.
	go m.prewarm(agentIDs)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				maintainCtx, cancel := context.WithTimeout(context.Background(), m.cfg.RequestTimeout)
				for _, pool := range m.pools {
					if err := pool.maintain(maintainCtx); err != nil {
						m.logger.Printf("%s: maintenance failed: %v", pool.name, err)
					}
				}
				cancel()
			case <-m.stopCh:
				return
			}
		}
	}()
}

func (m *RunManager) prewarm(agentIDs []string) {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.RequestTimeout)
	defer cancel()

	for _, pool := range m.pools {
		for _, agentID := range agentIDs {
			if err := pool.rotateAgent(ctx, agentID); err != nil {
				m.logger.Printf("%s: prewarm %s failed: %v", pool.name, agentID, err)
			} else {
				m.logger.Printf("%s: prewarmed %s", pool.name, agentID)
			}
		}
	}
}

func (m *RunManager) Close(ctx context.Context) {
	close(m.stopCh)
	m.wg.Wait()
	for _, pool := range m.pools {
		if err := pool.shutdown(ctx); err != nil {
			m.logger.Printf("%s: shutdown failed: %v", pool.name, err)
		}
	}
}

func (m *RunManager) Acquire(ctx context.Context, agentID string) (*runLease, error) {
	if len(m.pools) == 0 {
		return nil, errors.New("no auth tokens configured")
	}

	startIndex := int(m.next.Add(1)-1) % len(m.pools)
	var errs []string
	for offset := 0; offset < len(m.pools); offset++ {
		pool := m.pools[(startIndex+offset)%len(m.pools)]
		lease, err := pool.acquire(ctx, agentID)
		if err == nil {
			return lease, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", pool.name, err))
	}

	return nil, fmt.Errorf("unable to acquire run from any token (%s)", strings.Join(errs, "; "))
}

func (m *RunManager) Release(lease *runLease) {
	if lease == nil || lease.pool == nil || lease.run == nil {
		return
	}
	lease.pool.release(lease.run)
}

func (m *RunManager) Invalidate(lease *runLease, reason string) {
	if lease == nil || lease.pool == nil || lease.run == nil {
		return
	}
	lease.pool.invalidate(lease.run, reason)
}

func (m *RunManager) Cooldown(lease *runLease, duration time.Duration, reason string) {
	if lease == nil || lease.pool == nil {
		return
	}
	lease.pool.markCooldown(duration, reason)
}

func (m *RunManager) Snapshots() []tokenSnapshot {
	snapshots := make([]tokenSnapshot, 0, len(m.pools))
	for _, pool := range m.pools {
		snapshots = append(snapshots, pool.snapshot())
	}
	return snapshots
}

func (p *tokenPool) acquire(ctx context.Context, agentID string) (*runLease, error) {
	instanceID, err := p.ensureSessionReady(ctx)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if now := time.Now(); now.Before(p.cooldownUntil) {
		cooldownUntil := p.cooldownUntil
		p.mu.Unlock()
		return nil, fmt.Errorf("token cooling down until %s", cooldownUntil.Format(time.RFC3339))
	}
	run := p.runs[agentID]
	needsRotate := run == nil || time.Since(run.startedAt) >= p.cfg.RotationInterval
	p.mu.Unlock()

	if needsRotate {
		if err := p.rotateAgent(ctx, agentID); err != nil {
			return nil, err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	run = p.runs[agentID]
	if run == nil {
		return nil, errors.New("run missing after rotation")
	}
	run.inflight++
	run.requestCount++
	return &runLease{pool: p, run: run, freebuffInstanceID: instanceID}, nil
}

func (p *tokenPool) maintain(ctx context.Context) error {
	p.mu.Lock()
	var toRotate []string
	for agentID, run := range p.runs {
		if time.Since(run.startedAt) >= p.cfg.RotationInterval {
			toRotate = append(toRotate, agentID)
		}
	}
	draining := append([]*managedRun(nil), p.draining...)
	p.mu.Unlock()

	for _, agentID := range toRotate {
		if err := p.rotateAgent(ctx, agentID); err != nil {
			p.logger.Printf("%s: rotate agent %s failed: %v", p.name, agentID, err)
		}
	}

	for _, run := range draining {
		if err := p.finishIfReady(run); err != nil {
			p.logger.Printf("%s: finish draining run %s failed: %v", p.name, run.id, err)
		}
	}
	return nil
}

func (p *tokenPool) shutdown(ctx context.Context) error {
	p.mu.Lock()
	var allRuns []*managedRun
	for _, run := range p.runs {
		allRuns = append(allRuns, run)
	}
	allRuns = append(allRuns, p.draining...)
	p.runs = make(map[string]*managedRun)
	p.draining = nil
	p.mu.Unlock()

	var errs []string
	for _, run := range allRuns {
		if err := p.client.FinishRun(ctx, p.token, run.id, run.requestCount); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (p *tokenPool) rotateAgent(ctx context.Context, agentID string) error {
	p.mu.Lock()
	if now := time.Now(); now.Before(p.cooldownUntil) {
		cooldownUntil := p.cooldownUntil
		p.mu.Unlock()
		return fmt.Errorf("token cooling down until %s", cooldownUntil.Format(time.RFC3339))
	}
	p.mu.Unlock()

	runID, err := p.client.StartRun(ctx, p.token, agentID)
	if err != nil {
		p.mu.Lock()
		p.lastError = err.Error()
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	oldRun := p.runs[agentID]
	p.runs[agentID] = &managedRun{
		id:        runID,
		agentID:   agentID,
		startedAt: time.Now(),
	}
	p.lastError = ""
	if oldRun != nil {
		p.draining = append(p.draining, oldRun)
	}
	p.mu.Unlock()

	if oldRun != nil {
		go func(run *managedRun) {
			if err := p.finishIfReady(run); err != nil {
				p.logger.Printf("%s: finish rotated run %s (agent %s) failed: %v", p.name, run.id, run.agentID, err)
			}
		}(oldRun)
	}
	return nil
}

func (p *tokenPool) release(run *managedRun) {
	if run == nil {
		return
	}

	p.mu.Lock()
	if run.inflight > 0 {
		run.inflight--
	}
	p.mu.Unlock()

	if err := p.finishIfReady(run); err != nil {
		p.logger.Printf("%s: finish released run %s failed: %v", p.name, run.id, err)
	}
}

func (p *tokenPool) finishIfReady(run *managedRun) error {
	p.mu.Lock()
	if run == nil || run.inflight > 0 || run.finishing {
		p.mu.Unlock()
		return nil
	}
	// Only finish if this run is no longer the current run for its agent
	if current, ok := p.runs[run.agentID]; ok && current == run {
		p.mu.Unlock()
		return nil
	}
	run.finishing = true
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.RequestTimeout)
	defer cancel()

	if err := p.client.FinishRun(ctx, p.token, run.id, run.requestCount); err != nil {
		p.mu.Lock()
		run.finishing = false
		p.lastError = err.Error()
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	filtered := p.draining[:0]
	for _, drainingRun := range p.draining {
		if drainingRun != run {
			filtered = append(filtered, drainingRun)
		}
	}
	p.draining = filtered
	p.mu.Unlock()
	return nil
}

func (p *tokenPool) invalidate(run *managedRun, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove from current runs if it matches
	if current, ok := p.runs[run.agentID]; ok && current == run {
		delete(p.runs, run.agentID)
	}

	filtered := p.draining[:0]
	for _, drainingRun := range p.draining {
		if drainingRun != run {
			filtered = append(filtered, drainingRun)
		}
	}
	p.draining = filtered
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) markCooldown(duration time.Duration, reason string) {
	if duration <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cooldownUntil = time.Now().Add(duration)
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) invalidateSession(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessionStatus = ""
	p.sessionID = ""
	p.sessionPollAt = time.Time{}
	p.sessionExpiry = time.Time{}
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) ensureSessionReady(ctx context.Context) (string, error) {
	for {
		p.mu.Lock()
		status := p.sessionStatus
		instanceID := p.sessionID
		pollAt := p.sessionPollAt
		expiry := p.sessionExpiry
		p.mu.Unlock()

		if status == "disabled" {
			return "", nil
		}
		if status == "active" && strings.TrimSpace(instanceID) != "" {
			if expiry.IsZero() || time.Now().Before(expiry.Add(-5*time.Second)) {
				return instanceID, nil
			}
		}

		if !pollAt.IsZero() && time.Now().Before(pollAt) {
			waitFor := time.Until(pollAt)
			timer := time.NewTimer(waitFor)
			select {
			case <-ctx.Done():
				timer.Stop()
				return "", ctx.Err()
			case <-timer.C:
			}
		}

		var (
			state *FreeSessionState
			err   error
		)
		if strings.TrimSpace(instanceID) != "" && (status == "queued" || status == "active") {
			state, err = p.client.GetFreeSession(ctx, p.token, instanceID)
		} else {
			state, err = p.client.PostFreeSession(ctx, p.token)
		}
		if err != nil {
			p.mu.Lock()
			p.lastError = err.Error()
			p.sessionPollAt = time.Now().Add(5 * time.Second)
			p.mu.Unlock()
			return "", err
		}

		ready, nextInstanceID, delay := p.applyFreeSessionState(state)
		if ready {
			return nextInstanceID, nil
		}

		if delay <= 0 {
			delay = time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *tokenPool) applyFreeSessionState(state *FreeSessionState) (bool, string, time.Duration) {
	if state == nil {
		return false, "", time.Second
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.sessionStatus = strings.TrimSpace(state.Status)
	p.sessionID = strings.TrimSpace(state.InstanceID)
	p.sessionPollAt = time.Time{}
	p.sessionExpiry = state.ExpiresAt
	p.lastError = ""

	switch p.sessionStatus {
	case "disabled":
		p.sessionID = ""
		return true, "", 0
	case "active":
		if p.sessionID == "" {
			p.lastError = "free session active response missing instance id"
			return false, "", time.Second
		}
		return true, p.sessionID, 0
	case "queued":
		delay := time.Duration(state.EstimatedWaitMs) * time.Millisecond
		if delay < time.Second {
			delay = time.Second
		}
		if delay > 15*time.Second {
			delay = 15 * time.Second
		}
		p.sessionPollAt = time.Now().Add(delay)
		p.lastError = fmt.Sprintf("freebuff waiting room queued (position %d/%d)", maxInt(state.Position, 1), maxInt(state.QueueDepth, maxInt(state.Position, 1)))
		return false, "", delay
	case "none", "ended", "superseded":
		return false, "", 0
	default:
		if state.Message != "" {
			p.lastError = state.Message
		} else {
			p.lastError = "unexpected free session status: " + p.sessionStatus
		}
		return false, "", time.Second
	}
}

func (p *tokenPool) snapshot() tokenSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	snapshot := tokenSnapshot{
		Name:          p.name,
		DrainingRuns:  len(p.draining),
		CooldownUntil: p.cooldownUntil,
		LastError:     p.lastError,
	}
	for agentID, run := range p.runs {
		snapshot.Runs = append(snapshot.Runs, runSnapshot{
			AgentID:      agentID,
			RunID:        run.id,
			StartedAt:    run.startedAt,
			Inflight:     run.inflight,
			RequestCount: run.requestCount,
		})
	}
	return snapshot
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
