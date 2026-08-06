// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Script} from "forge-std/Script.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";

/// @notice 部署 ArbitrageExecutor。
///         用法（真实链，M0 地址见 deployments/M0-chain-facts.md）：
///           export PRIVATE_KEY=... HOT_WALLET=0x...   # 私钥仅环境变量，不进仓库
///           forge script script/DeployArbitrage.s.sol:DeployArbitrage \
///             --rpc-url https://rpc.mainnet.chain.robinhood.com --broadcast --legacy
///         部署后注资少量 WETH 到合约（预检要求 balance > 0）：
///           cast send <contract> "deposit()" --value 0.001ether ...
contract DeployArbitrage is Script {
    function run() external {
        address hotWallet = vm.envAddress("HOT_WALLET");
        // M0 实测地址（见 deployments/M0-chain-facts.md）
        address weth = vm.envAddress("WETH");
        address factory = vm.envAddress("V3_FACTORY");

        vm.startBroadcast();
        new ArbitrageExecutor(hotWallet, weth, factory);
        vm.stopBroadcast();
    }
}
