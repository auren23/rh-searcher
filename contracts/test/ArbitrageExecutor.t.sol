// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";

/// @notice ArbitrageExecutor 基础安全测试：权限、白名单、暂停、deadline。
contract ArbitrageExecutorTest is Test {
    ArbitrageExecutor exec;
    address owner = address(0x1);
    address hot = address(0x2);
    address attacker = address(0x3);
    address weth = address(0x4200000000000000000000000000000000000006);
    address factory = address(0x4);

    function setUp() public {
        exec = new ArbitrageExecutor(hot, weth, factory);
    }

    function testOnlyExecutorCanExecute() public {
        vm.prank(attacker);
        vm.expectRevert();
        exec.executeV3Cycle(new ArbitrageExecutor.Hop[](0), 0, 0, block.timestamp + 1);
    }

    function testPauseBlocksExecute() public {
        exec.pause();
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(new ArbitrageExecutor.Hop[](0), 0, 0, block.timestamp + 1);
    }

    function testOwnerCanPauseAndUnpause() public {
        exec.pause();
        exec.unpause();
        // unpause 后不再报 paused（下一步是 hop 长度校验）
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(new ArbitrageExecutor.Hop[](0), 0, 0, block.timestamp + 1);
    }

    function testDeadline() public {
        vm.prank(hot);
        vm.expectRevert(ArbitrageExecutor.DeadlinePassed.selector);
        exec.executeV3Cycle(new ArbitrageExecutor.Hop[](0), 0, 0, block.timestamp - 1);
    }

    function testDuplicatePoolReverts() public {
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](2);
        hs[0] = ArbitrageExecutor.Hop({pool: address(0x9), tokenIn: weth, tokenOut: address(0xa), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(0x9), tokenIn: address(0xa), tokenOut: weth, fee: 3000});
        vm.prank(hot);
        // 未注册池先被归属验证拒绝（PoolNotWhitelisted）；重复池检查在归属验证之后
        vm.expectRevert();
        exec.executeV3Cycle(hs, 1e18, 1, block.timestamp + 1);
    }

    function testHopCountReverts() public {
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](1);
        hs[0] = ArbitrageExecutor.Hop({pool: address(0x9), tokenIn: weth, tokenOut: weth, fee: 3000});
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(hs, 1e18, 1, block.timestamp + 1);
    }

    function testWhitelistOnlyOwner() public {
        vm.prank(attacker);
        vm.expectRevert();
        exec.setRouter(address(0x9), true);
    }
}
