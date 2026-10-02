//! Detection: threshold rules and anomaly detection producing findings (M2).

/// Exponentially weighted moving average and variance for streaming anomaly scoring.
#[derive(Debug, Clone, Copy)]
pub struct Ewma {
    alpha: f64,
    mean: Option<f64>,
    var: f64,
}

impl Ewma {
    /// `alpha` in `(0, 1]`; higher reacts faster to recent samples.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(f64::EPSILON, 1.0),
            mean: None,
            var: 0.0,
        }
    }

    /// Feed a sample and return its z-score against the state before the update.
    pub fn observe(&mut self, x: f64) -> f64 {
        let Some(mean) = self.mean else {
            self.mean = Some(x);
            return 0.0;
        };
        let std = self.var.sqrt();
        let z = if std > f64::EPSILON {
            (x - mean) / std
        } else {
            0.0
        };
        let diff = x - mean;
        let incr = self.alpha * diff;
        self.mean = Some(mean + incr);
        self.var = (1.0 - self.alpha) * (self.var + diff * incr);
        z
    }

    pub fn mean(&self) -> Option<f64> {
        self.mean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spike_scores_high_after_stable_series() {
        let mut e = Ewma::new(0.3);
        for i in 0..50 {
            e.observe(10.0 + (i % 2) as f64 * 0.5);
        }
        assert!(e.observe(30.0) > 5.0);
    }
}
