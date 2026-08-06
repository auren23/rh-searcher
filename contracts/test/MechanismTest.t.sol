// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {FixtureWETH} from "../src/testnet/FixtureWETH.sol";
import {FixtureFactory} from "../src/testnet/FixtureFactory.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";
import {UniswapV3Factory} from "v3core08/UniswapV3Factory.sol";
import {IUniswapV3Factory} from "v3core08/interfaces/IUniswapV3Factory.sol";

/// @notice 机制验证：真实 V3 池 + 价差 + ArbitrageExecutor 全链路。
/// 两池同一 token 对（fee 500/3000）价格不同 → WETH→T→WETH 环获利。
contract MechanismTest is Test {
    FixtureWETH weth;
    FixtureFactory fx;
    ArbitrageExecutor exec;
    IUniswapV3Factory factory;
    address token;

    function setUp() public {
        weth = new FixtureWETH();
        // 第二个 token：用 WETH 的简化版（同名不同实例即可）
        token = address(new FixtureWETH());
        // 部署 V3 factory
        vm.broadcast(address(this));
        factory = IUniswapV3Factory(address(new UniswapV3FactoryForTest()));
        fx = new FixtureFactory();
        fx.setup(address(factory), address(weth), token);
        // 铸币：minter 需要两种 token
        weth.deposit{value: 200 ether}();
        FixtureWETH(payable(token)).deposit{value: 200 ether}();
        weth.transfer(address(fx), 100 ether);
        FixtureWETH(payable(token)).transfer(address(fx), 100 ether);
        fx.mintFullRange(4e19); // 与 5e19 token 注资匹配（全范围 amount0≈amount1≈L）
        // 部署 executor + 注资
        exec = new ArbitrageExecutor(address(this), address(weth), address(factory));
        weth.transfer(address(exec), 10 ether);
    }

    function testArbitrageCycleProfits() public {
        (uint256 r0a, uint256 r1a) = fx.reserves(address(fx.poolA()));
        (uint256 r0b, uint256 r1b) = fx.reserves(address(fx.poolB()));
        // 池 A 便宜（价 1.0）池 B 贵（价 1.02）：WETH→T 走 A，T→WETH 走 B
        uint256 amountIn = 1e16; // 0.01 WETH
        // 环：WETH→X（poolA，费 500）→WETH（poolB，费 3000）。
        // 方向由合约按 tokenIn 与池 token0/token1 关系决定
        address x = address(fx.poolA().token0()) == address(weth)
            ? address(fx.poolA().token1())
            : address(fx.poolA().token0());
        ArbitrageExecutor.Hop[] memory hops = new ArbitrageExecutor.Hop[](2);
        hops[0] = ArbitrageExecutor.Hop(address(fx.poolA()), address(weth), x, 500);
        hops[1] = ArbitrageExecutor.Hop(address(fx.poolB()), x, address(weth), 3000);
        uint256 profit = exec.executeV3Cycle(hops, amountIn, 0, block.timestamp + 100);
        assertGt(profit, 0, "cycle must profit");
    }

    function testRevertWhenMinProfitTooHigh() public {
        address x = address(fx.poolA().token0()) == address(weth)
            ? address(fx.poolA().token1())
            : address(fx.poolA().token0());
        ArbitrageExecutor.Hop[] memory hops = new ArbitrageExecutor.Hop[](2);
        hops[0] = ArbitrageExecutor.Hop(address(fx.poolA()), address(weth), x, 500);
        hops[1] = ArbitrageExecutor.Hop(address(fx.poolB()), x, address(weth), 3000);
        vm.expectRevert();
        exec.executeV3Cycle(hops, 1e16, type(uint256).max, block.timestamp + 100);
    }

    function testOnlyExecutorCanCall() public {
        address x = address(fx.poolA().token0()) == address(weth)
            ? address(fx.poolA().token1())
            : address(fx.poolA().token0());
        ArbitrageExecutor.Hop[] memory hops = new ArbitrageExecutor.Hop[](2);
        hops[0] = ArbitrageExecutor.Hop(address(fx.poolA()), address(weth), x, 500);
        hops[1] = ArbitrageExecutor.Hop(address(fx.poolB()), x, address(weth), 3000);
        vm.prank(address(0xdead));
        vm.expectRevert();
        exec.executeV3Cycle(hops, 1e16, 0, block.timestamp + 100);
    }
}

// 包装：V3 factory 构造（需要 deployer）
contract UniswapV3FactoryForTest is UniswapV3Factory {
    constructor() {}
}
