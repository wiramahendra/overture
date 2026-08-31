package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// First-class execution metrics — Action -> Run -> Proof
var (
	OvertureTaskSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_task_submitted_total",
		Help: "Total durable task submissions",
	}, []string{"tenant", "action"})

	OvertureTaskDispatched = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_task_dispatched_total",
		Help: "Tasks dispatched to runtime",
	}, []string{"tenant", "runtime"})

	OvertureCheckpointAccepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_checkpoint_accepted_total",
		Help: "Checkpoints accepted and persisted",
	}, []string{"tenant"})

	OvertureRecoveryStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_recovery_started_total",
		Help: "Recovery attempts started for dead runtimes",
	}, []string{"tenant", "reason"})

	OvertureHandoffDenied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_handoff_denied_total",
		Help: "Recovery handoffs denied (irreversible / approval required)",
	}, []string{"tenant", "reason"})

	OvertureDLQEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_dlq_enqueued_total",
		Help: "Tasks moved to dead-letter after max recovery attempts",
	}, []string{"tenant"})

	OvertureProofVerified = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "overture_proof_verified_total",
		Help: "Proof verifications by outcome",
	}, []string{"tenant", "outcome"}) // outcome: verified|mismatch|present

	OvertureRunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "overture_run_duration_seconds",
		Help:    "Run duration from submit to terminal state",
		Buckets: prometheus.DefBuckets,
	}, []string{"action", "status"})

	OvertureRecoveryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "overture_recovery_latency_seconds",
		Help:    "Time from runtime death to recovery dispatch",
		Buckets: prometheus.DefBuckets,
	}, []string{"tenant"})
)

// Record helpers — safe to call from coordinator without import cycle via direct prometheus use
func RecordTaskSubmitted(tenant, action string) { OvertureTaskSubmitted.WithLabelValues(tenant, action).Inc() }
func RecordTaskDispatched(tenant, runtime string) { OvertureTaskDispatched.WithLabelValues(tenant, runtime).Inc() }
func RecordCheckpointAccepted(tenant string) { OvertureCheckpointAccepted.WithLabelValues(tenant).Inc() }
func RecordRecoveryStarted(tenant, reason string) { OvertureRecoveryStarted.WithLabelValues(tenant, reason).Inc() }
func RecordHandoffDenied(tenant, reason string) { OvertureHandoffDenied.WithLabelValues(tenant, reason).Inc() }
func RecordDLQEnqueued(tenant string) { OvertureDLQEnqueued.WithLabelValues(tenant).Inc() }
func RecordProofVerified(tenant, outcome string) { OvertureProofVerified.WithLabelValues(tenant, outcome).Inc() }
