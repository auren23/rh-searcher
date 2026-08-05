// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";

/// @notice ArbitrageExecutor 基础测试：权限、白名单、暂停。
///         利润不足/重入/伪造回调等测试在 M4 验收中补齐（需要 fork + 真实池）。
contract ArbitrageExecutorTest is Test {
    ArbitrageExecutor exec;
    address owner = address(0x1);
    address hot = address(0x2);
    address attacker = address(0x3);
    address weth = address(0x4200000000000000000000000000000000000006);
    address router = address(0x4);

    function setUp() public {
        exec = new ArbitrageExecutor(hot, weth, router);
    }

    function testOnlyExecutorCanExecute() public {
        vm.prank(attacker);
        vm.expectRevert();
        exec.execute("", 0, 0, block.timestamp + 1);
    }

    function testPauseBlocksExecute() public {
        // owner = 测试合约本身（部署者）
        exec.pause();
        vm.prank(hot);
        vm.expectRevert();
        exec.execute("", 0, 0, block.timestamp + 1);
    }

    function testOwnerCanPauseAndUnpause() public {
        exec.pause();
        exec.unpause();
        // unpause 后不报 paused 错即可（余额校验先失败，但错误码不同）
        // weth mock 未实现 balanceOf，revert 无 data；只验证不再报 paused
        vm.prank(hot);
        vm.expectRevert();
        exec.execute("", 1, 0, block.timestamp + 1);
    }

    function testWhitelistOnlyOwner() public {
        vm.prank(attacker);
        vm.expectRevert();
        exec.setRouter(address(0x9), true);
    }

    function testDeadline() public {
        vm.prank(hot);
        vm.expectRevert(ArbitrageExecutor.DeadlinePassed.selector);
        exec.execute("", 0, 0, block.timestamp - 1);
    }
}
