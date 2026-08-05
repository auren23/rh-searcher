// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {BaseExecutor} from "./BaseExecutor.sol";

/// @title ArbitrageExecutor
/// @notice WETH 循环套利执行合约：直接调用 V3 Pool.swap()，不依赖 Router/Permit2。
/// @dev 流程：
///       V3Pool A.swap() → uniswapV3SwapCallback() 支付 WETH 收 TOKEN
///       → V3Pool B.swap() → callback 支付 TOKEN 收 WETH
///       → 检查 WETH 余额差 >= minProfit
///       回调中验证 msg.sender == 期望池 且 factory.getPool(token0, token1, fee) == msg.sender。
interface IWETH {
    function deposit() external payable;
    function withdraw(uint256) external;
    function transfer(address, uint256) external returns (bool);
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
}

interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
    function transfer(address, uint256) external returns (bool);
    function transferFrom(address, address, uint256) external returns (bool);
}

interface IUniswapV3Factory {
    function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address);
}

interface IUniswapV3Pool {
    function swap(
        address recipient,
        bool zeroForOne,
        int256 amountSpecified,
        uint160 sqrtPriceLimitX96,
        bytes calldata data
    ) external returns (int256 amount0, int256 amount1);
}

contract ArbitrageExecutor is BaseExecutor {
    IWETH public immutable weth;
    IUniswapV3Factory public immutable factory;

    uint256 public constant MIN_SQRT_RATIO = 4295128739;
    uint256 public constant MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342;
    uint256 public constant MAX_HOPS = 3;

    uint256 private _locked; // 1 = 执行中（nonReentrant）

    error InsufficientProfit(uint256 actual, uint256 min);
    error DeadlinePassed();
    error InvalidCallback(address caller);
    error PoolNotWhitelisted(address pool);
    error HopDiscontinuity(uint256 index);
    error DuplicatePool(address pool);
    error Reentrancy();

    struct Hop {
        address pool;     // V3 池地址
        address tokenIn;
        address tokenOut;
        uint24 fee;
    }

    /// @notice 执行中的 hops（回调可见；执行结束清除）。
    Hop[] private _hops;

    modifier nonReentrant() {
        if (_locked != 0) revert Reentrancy();
        _locked = 1;
        _;
        _locked = 0;
    }

    constructor(address executor_, address weth_, address factory_) BaseExecutor(executor_) {
        weth = IWETH(weth_);
        factory = IUniswapV3Factory(factory_);
    }

    /// @notice 执行 WETH 循环套利（直接调池，两跳起步）。
    /// @param hops 按执行顺序的跳（WETH 开始、WETH 结束、相邻跳 token 衔接、池不重复）
    /// @param amountIn 初始 WETH 输入
    /// @param minProfit 最小净 WETH 利润，不足 revert
    /// @param deadline 过期时间戳
    function executeV3Cycle(
        Hop[] calldata hops,
        uint256 amountIn,
        uint256 minProfit,
        uint256 deadline
    ) external onlyExecutor whenNotPaused nonReentrant returns (uint256 profit) {
        if (block.timestamp > deadline) revert DeadlinePassed();
        if (hops.length < 2 || hops.length > MAX_HOPS) revert HopDiscontinuity(hops.length);

        // 路由校验：起止 WETH、相邻衔接、池不重复
        if (hops[0].tokenIn != address(weth) || hops[hops.length - 1].tokenOut != address(weth)) {
            revert HopDiscontinuity(0);
        }
        for (uint256 i = 0; i < hops.length; i++) {
            if (i > 0 && hops[i - 1].tokenOut != hops[i].tokenIn) revert HopDiscontinuity(i);
            for (uint256 j = 0; j < i; j++) {
                if (hops[j].pool == hops[i].pool) revert DuplicatePool(hops[i].pool);
            }
            // 池归属验证：factory.getPool(tokenIn, tokenOut, fee) == pool
            address expected = factory.getPool(hops[i].tokenIn, hops[i].tokenOut, hops[i].fee);
            if (expected != hops[i].pool) revert PoolNotWhitelisted(hops[i].pool);
        }

        uint256 wethBefore = weth.balanceOf(address(this));
        if (wethBefore < amountIn) revert InsufficientProfit(wethBefore, amountIn);

        // 复制到 storage（回调可见）
        delete _hops;
        for (uint256 i = 0; i < hops.length; i++) {
            _hops.push(hops[i]);
        }

        // 逐跳执行：调池 swap（回调中主动付款，无需 approve）
        uint256 current = amountIn;
        for (uint256 i = 0; i < _hops.length; i++) {
            address tokenIn = _hops[i].tokenIn;
            // 方向：tokenIn 若为池 token0 → zeroForOne=true
            bool zeroForOne;
            (address t0, address t1) = poolTokens(_hops[i].pool);
            zeroForOne = tokenIn == t0;
            if (tokenIn != t0 && tokenIn != t1) revert PoolNotWhitelisted(_hops[i].pool);

            uint160 limit = zeroForOne ? uint160(MIN_SQRT_RATIO + 1) : uint160(MAX_SQRT_RATIO - 1);
            // V3 语义：amountSpecified > 0 = exact input（与方向无关），方向只由 zeroForOne 决定
            int256 amountSpecified = int256(uint256(current));
            uint256 outBefore = IERC20(_hops[i].tokenOut).balanceOf(address(this));
            IUniswapV3Pool(_hops[i].pool).swap(address(this), zeroForOne, amountSpecified, limit, abi.encode(i));
            // 本跳输出 = 接收后余额差（不能用绝对余额：可能叠加历史持仓）
            uint256 outAfter = IERC20(_hops[i].tokenOut).balanceOf(address(this));
            require(outAfter >= outBefore, "hop-output-negative");
            current = outAfter - outBefore;
        }

        // 结束时以 WETH 余额差验证（不信任中间结果）
        uint256 wethAfter = weth.balanceOf(address(this));
        profit = wethAfter - wethBefore;
        if (profit < minProfit) revert InsufficientProfit(profit, minProfit);

        // 清除临时执行状态
        delete _hops;
    }

    /// @dev V3 池回调：验证调用者是期望池 + factory 归属，然后主动支付正的 delta。
    ///      真实 V3 语义：池在回调返回后检查自身余额增加（balanceAfter >= balanceBefore + delta），
    ///      因此回调方必须在这里把 token 转给池。
    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external {
        if (_locked == 0) revert Reentrancy();
        (uint256 hopIndex) = abi.decode(data, (uint256));
        if (hopIndex >= _hops.length) revert InvalidCallback(msg.sender);
        Hop memory h = _hops[hopIndex];

        // 双重验证：msg.sender 是期望池 + factory.getPool 确认归属
        if (msg.sender != h.pool) revert InvalidCallback(msg.sender);
        if (factory.getPool(h.tokenIn, h.tokenOut, h.fee) != h.pool) revert PoolNotWhitelisted(msg.sender);

        // 以池的 token0/token1 为准付款（正 delta 的一方是池应收的）
        (address token0, address token1) = poolTokens(msg.sender);
        if (amount0Delta > 0) {
            require(IERC20(token0).transfer(msg.sender, uint256(amount0Delta)), "pay0-failed");
        }
        if (amount1Delta > 0) {
            require(IERC20(token1).transfer(msg.sender, uint256(amount1Delta)), "pay1-failed");
        }
    }

    /// @dev 读取池的 token0/token1（回调方向判定用）。
    function poolTokens(address pool) internal view returns (address, address) {
        (bool ok0, bytes memory r0) = pool.staticcall(abi.encodeWithSignature("token0()"));
        (bool ok1, bytes memory r1) = pool.staticcall(abi.encodeWithSignature("token1()"));
        if (!ok0 || !ok1 || r0.length < 32 || r1.length < 32) revert PoolNotWhitelisted(pool);
        return (abi.decode(r0, (address)), abi.decode(r1, (address)));
    }

    /// @dev 接收 ETH（MVP 未用，防误发）。
    receive() external payable {}
}
