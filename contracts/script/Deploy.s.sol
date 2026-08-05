// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Script} from "forge-std/Script.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";
import {LiquidationExecutor} from "../src/LiquidationExecutor.sol";

/// @notice 部署脚本：forge script script/Deploy.s.sol:Deploy --rpc-url $RPC --broadcast
///         热钱包地址通过 env HOT_WALLET 传入；合约地址写入 deployments/。
contract Deploy is Script {
    function run() external {
        address hotWallet = vm.envAddress("HOT_WALLET");
        address weth = vm.envAddress("WETH");
        address router = vm.envAddress("ROUTER");
        address morpho = vm.envAddress("MORPHO");

        vm.startBroadcast();
        new ArbitrageExecutor(hotWallet, weth, router);
        new LiquidationExecutor(hotWallet, morpho);
        vm.stopBroadcast();
    }
}
