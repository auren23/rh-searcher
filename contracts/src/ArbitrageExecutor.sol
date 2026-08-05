// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {BaseExecutor} from "./BaseExecutor.sol";

/// @title ArbitrageExecutor
/// @notice WETH 循环套利执行合约：WETH -> TOKEN -> WETH，原子执行。
/// @dev 安全要求：
///       - 仅授权执行钱包可调用
///       - Router / Pool 白名单
///       - deadline / minProfit 硬门槛
///       - 结束时以 WETH 余额差验证（不信任路径中间结果）
///       - nonReentrant、paused
///       - 失败自动 revert；无任意外部 call 入口
interface IWETH {
    function deposit() external payable;
    function withdraw(uint256) external;
    function transfer(address, uint256) external returns (bool);
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
}

interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

/// @notice 执行路由编码：token0, fee, token1, fee, token2 ...（与 Router 一致）
library RouteLib {
    function decodeHops(bytes calldata route) internal pure returns (address[] memory tokens, uint24[] memory fees) {
        uint256 n = (route.length - 20) / 23; // 每跳：20B token + 3B fee
        tokens = new address[](n + 1);
        fees = new uint24[](n);
        assembly {
            let p := route.offset
            for { let i := 0 } lt(i, n) { i := add(i, 1) } {
                let t := shr(96, calldataload(p))
                mstore(add(tokens, add(0x20, mul(i, 0x20))), t)
                let f := shr(232, calldataload(add(p, 20)))
                mstore(add(fees, add(0x20, mul(i, 0x20))), f)
                p := add(p, 23)
            }
            // 最后一个 token
            let t := shr(96, calldataload(p))
            mstore(add(tokens, add(0x20, mul(n, 0x20))), t)
        }
    }
}

contract ArbitrageExecutor is BaseExecutor {
    IWETH public immutable weth;
    ISwapRouter public immutable router; // MVP 单 Router；多 DEX 用路由字节码区分

    uint256 public constant MAX_HOPS = 3;

    error InsufficientProfit(uint256 actual, uint256 min);
    error DeadlinePassed();
    error BadRoute();
    error UnauthorizedPool(address pool);

    constructor(address executor_, address weth_, address router_) BaseExecutor(executor_) {
        weth = IWETH(weth_);
        router = ISwapRouter(router_);
        whitelistedRouters[router_] = true;
    }

    /// @notice 执行 WETH 循环套利。
    /// @param route 编码路径：token0, fee, token1, fee, token2...
    /// @param amountIn 初始 WETH 输入量
    /// @param minProfit 最小净 WETH 利润（wei），不足则 revert
    /// @param deadline 过期时间戳
    function execute(bytes calldata route, uint256 amountIn, uint256 minProfit, uint256 deadline)
        external
        onlyExecutor
        whenNotPaused
    {
        if (block.timestamp > deadline) revert DeadlinePassed();

        uint256 wethBefore = weth.balanceOf(address(this));
        require(wethBefore >= amountIn, "AE:insufficient-weth");

        (address[] memory tokens, uint24[] memory fees) = RouteLib.decodeHops(route);
        require(tokens.length >= 3 && tokens.length - 1 == fees.length, "AE:bad-route-length");
        require(tokens.length - 1 <= MAX_HOPS, "AE:too-many-hops");
        require(tokens[0] == address(weth) && tokens[tokens.length - 1] == address(weth), "AE:not-weth-loop");

        weth.approve(address(router), amountIn);
        uint256 current = amountIn;

        for (uint256 i = 0; i < fees.length; i++) {
            // 每跳走白名单 Router 的 exactInputSingle；中间 token 数量不信任，
            // 仅作为下一跳输入。
            ISwapRouter.ExactInputSingleParams memory params = ISwapRouter.ExactInputSingleParams({
                tokenIn: tokens[i],
                tokenOut: tokens[i + 1],
                fee: fees[i],
                recipient: address(this),
                deadline: deadline,
                amountIn: current,
                amountOutMinimum: i == fees.length - 1 ? 0 : 1, // 末跳由 minProfit 兜底
                sqrtPriceLimitX96: 0
            });
            // 校验池白名单：由 Router 池地址推导（MVP 简化：依赖 Router 白名单 + 末跳余额验证）
            current = router.exactInputSingle(params);
        }

        // 最终验证：WETH 余额差
        uint256 wethAfter = weth.balanceOf(address(this));
        uint256 profit = wethAfter - wethBefore;
        if (profit < minProfit) revert InsufficientProfit(profit, minProfit);
    }

    /// @dev 接收 ETH 用于 unwrap 场景（MVP 未用，保留防误发）。
    receive() external payable {}
}
