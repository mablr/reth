use super::ress_args::RessArgs;
use clap::Args;
use reth_chainspec::ChainSpec;
use reth_ethereum_forks::{EthereumHardfork, ForkCondition};

/// A trait that allows CLI extension args to apply hardfork overrides to a chain spec.
pub trait ApplyHardforkOverrides<CS> {
    /// Mutates the provided chain spec clone by applying any overrides.
    fn apply_hardfork_overrides(&self, _spec: &mut CS) {}
}

/// CLI args to override Ethereum hardfork activations.
#[derive(Debug, Clone, Default, Args, PartialEq, Eq)]
#[command(next_help_heading = "Hardfork Overrides")]
pub struct EthereumHardforkOverrideArgs {
    /// Override Prague hardfork activation timestamp
    #[arg(long = "override.prague", value_name = "TIMESTAMP")]
    pub prague: Option<u64>,
}

impl EthereumHardforkOverrideArgs {
    /// Apply the overrides to the provided hardfork mutator closure.
    pub fn apply_to<F>(&self, mut set: F)
    where
        F: FnMut(EthereumHardfork, ForkCondition),
    {
        if let Some(ts) = self.prague {
            set(EthereumHardfork::Prague, ForkCondition::Timestamp(ts));
        }
    }
}

/// Composite ETH ext args: existing `RessArgs` plus hardfork overrides.
#[derive(Debug, Clone, Default, Args)]
pub struct EthereumExtArgs {
    /// Ress subprotocol arguments
    #[command(flatten)]
    pub ress: RessArgs,
    /// Ethereum hardfork override arguments
    #[command(flatten)]
    pub overrides: EthereumHardforkOverrideArgs,
}

impl ApplyHardforkOverrides<ChainSpec> for EthereumHardforkOverrideArgs {
    fn apply_hardfork_overrides(&self, spec: &mut ChainSpec) {
        self.apply_to(|hf, cond| {
            spec.hardforks.insert(hf, cond);
        });
    }
}

// OP-specific application is implemented in the OP crate where `OpChainSpec` is available.

impl ApplyHardforkOverrides<ChainSpec> for EthereumExtArgs {
    fn apply_hardfork_overrides(&self, spec: &mut ChainSpec) {
        self.overrides.apply_hardfork_overrides(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Args, Parser};

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[command(flatten)]
        args: T,
    }

    #[test]
    fn test_parse_eth_hardfork_overrides() {
        let expected_args = EthereumHardforkOverrideArgs { prague: Some(1) };
        let args = CommandParser::<EthereumHardforkOverrideArgs>::parse_from([
            "reth",
            "--override.prague",
            "1",
        ])
        .args;
        assert_eq!(args, expected_args);
    }
}
