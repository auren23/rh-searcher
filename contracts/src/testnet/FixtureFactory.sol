// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {IUniswapV3Pool} from "v3core08/interfaces/IUniswapV3Pool.sol";
import {IERC20Minimal} from "v3core08/interfaces/IERC20Minimal.sol";
import {TickMath} from "v3core08/libraries/TickMath.sol";
import {FixedPoint96} from "v3core08/libraries/FixedPoint96.sol";

/// @notice testnet 机制验证工具：创建带价差的 V3 池对 + 铸造流动性。
/// 两池同一 token 对、不同 fee（V3 factory 允许），价格不同 → 环可套利。
contract FixtureFactory {
    IUniswapV3Pool public poolA; // fee 500，价格较低（买入便宜）
    IUniswapV3Pool public poolB; // fee 3000，价格较高（卖出贵）
    address public token0;
    address public token1;

    /// @notice 创建两池 + 初始化价格（priceA < priceB 制造价差）
    function setup(address factory, address weth, address token) external {
        (address t0, address t1) = weth < token ? (weth, token) : (token, weth);
        token0 = t0;
        token1 = t1;
        poolA = IUniswapV3Pool(factoryCreatePool(factory, t0, t1, 500));
        poolB = IUniswapV3Pool(factoryCreatePool(factory, t0, t1, 3000));
        // 价格：token1/token0。A 便宜 B 贵。
        poolA.initialize(uint160(sqrtPrice(1.0e18)));
        poolB.initialize(uint160(sqrtPrice(1.02e18)));
    }

    function factoryCreatePool(address factory, address t0, address t1, uint24 fee)
        internal
        returns (address)
    {
        (bool ok, bytes memory ret) = factory.call(
            abi.encodeWithSignature("createPool(address,address,uint24)", t0, t1, fee));
        require(ok, "createPool failed");
        return abi.decode(ret, (address));
    }

    /// @notice 铸币（持有者需先给本合约转账两种 token）
    function mintRange(int24 lo, int24 hi, uint128 amount) external {
        poolA.mint(address(this), lo, hi, amount, "");
        poolB.mint(address(this), lo, hi, amount, "");
    }

    // 全范围：必须是 tickSpacing 的倍数（-887272 不是 60 的倍数，flipTick 会拒绝）
    int24 public constant FULL_LOWER = -887220;
    int24 public constant FULL_UPPER = 887220;

    function mintFullRange(uint128 amount) external {
        poolA.mint(address(this), FULL_LOWER, FULL_UPPER, amount, "");
        poolB.mint(address(this), FULL_LOWER, FULL_UPPER, amount, "");
    }

    function uniswapV3MintCallback(uint256 amount0, uint256 amount1, bytes calldata) external {
        IERC20Minimal(token0).transfer(msg.sender, amount0);
        IERC20Minimal(token1).transfer(msg.sender, amount1);
    }

    // 当前 token0 的储备（流动性代理）
    function reserves(address pool) external view returns (uint256 r0, uint256 r1) {
        r0 = IERC20Minimal(token0).balanceOf(pool);
        r1 = IERC20Minimal(token1).balanceOf(pool);
    }

    /// @notice sqrt(price) 转 Q96：price 以 1e18 表示（1.0e18 = 平价）
    function sqrtPrice(uint256 price1e18) public pure returns (uint256) {
        // sqrt(price * 2^96 * 2^96 / 2^18) 近似：sqrt(price1e18 * 2^174)
        // 用整数牛顿迭代求 sqrt(price1e18) 再放大到 Q96
        uint256 s = sqrt(price1e18); // 1e9 精度
        return (s << 96) / 1e9;
    }

    function sqrt(uint256 x) internal pure returns (uint256 y) {
        uint256 z = (x + 1) / 2;
        y = x;
        while (z < y) {
            y = z;
            z = (x / z + z) / 2;
        }
    }
}
