//! Cubic CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model based on cubic interpolation between the minimum and maximum power consumption values.
#[derive(Clone)]
pub struct CubicCpuPowerModel {
    #[allow(dead_code)]
    max_power: f64,
    min_power: f64,
    factor: f64,
}

impl CubicCpuPowerModel {
    /// Creates a cubic power model.
    ///
    /// * `max_power` - The maximum power consumption in W (at 100% utilization).
    /// * `min_power` - The minimum power consumption in W (at 0% utilization).
    pub fn new(max_power: f64, min_power: f64) -> Self {
        Self {
            min_power,
            max_power,
            factor: max_power - min_power,
        }
    }
}

impl CpuPowerModel for CubicCpuPowerModel {
    fn get_power(&self, utilization: f64) -> f64 {
        self.min_power + self.factor * utilization.powf(3.)
    }
}
