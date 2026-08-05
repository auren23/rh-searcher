// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test} from "forge-std/Test.sol";
import {ArbitrageExecutor} from "../src/ArbitrageExecutor.sol";

// 简化 ERC20
contract MockToken {
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    uint8 public immutable decimals = 18;
    string public name = "MockToken";
    string public symbol = "MTK";

    constructor() {
        balanceOf[msg.sender] = 1_000_000e18;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        allowance[from][msg.sender] -= amount;
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function mint(address to, uint256 amount) external {
        balanceOf[to] += amount;
    }
}

// 模拟 V3 池：swap() 回调 + 拉款 + 转账。exchangeRate = 池给 tokenOut 的倍数。
contract MockPool {
    MockToken public token0;
    MockToken public token1;
    uint24 public immutable fee;
    uint256 public exchangeRate; // tokenOut 倍数（如 10002/10000 表示 1.0002x 回报）

    constructor(MockToken t0, MockToken t1, uint24 fee_, uint256 exchangeRate_) {
        token0 = t0;
        token1 = t1;
        fee = fee_;
        exchangeRate = exchangeRate_;
    }

    function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160, bytes calldata data)
        external
        returns (int256, int256)
    {
        uint256 amountIn = uint256(amountSpecified < 0 ? -amountSpecified : amountSpecified);
        MockToken inTok = zeroForOne ? token0 : token1;
        MockToken outTok = zeroForOne ? token1 : token0;
        uint256 amountOut = amountIn * exchangeRate / 1e4;

        // 模拟 V3：先回调（池要求付款），再拉款、转出
        (bool ok,) = recipient.call(
            abi.encodeWithSignature("uniswapV3SwapCallback(int256,int256,bytes)",
                zeroForOne ? int256(uint256(amountIn)) : int256(0),
                zeroForOne ? int256(0) : int256(uint256(amountIn)),
                data)
        );
        require(ok, "callback failed");
        inTok.transferFrom(recipient, address(this), amountIn);
        outTok.transfer(recipient, amountOut);
        return (
            zeroForOne ? int256(uint256(amountIn)) : -int256(amountOut),
            zeroForOne ? -int256(amountOut) : int256(uint256(amountIn))
        );
    }
}

contract MockFactory {
    function getPool(address a, address b, uint24) external view returns (address) {
        // 测试用：返回任意地址无法验证；改为可配置
        return pools[a][b];
    }
    mapping(address => mapping(address => address)) public pools;
    function setPool(address a, address b, address p) external {
        pools[a][b] = p; // 单向注册（真实 factory 按 (tokenA, tokenB, fee) 寻址）
    }
}

contract ArbitrageExecutorV3CycleTest is Test {
    ArbitrageExecutor exec;
    MockToken weth;
    MockToken token;
    MockPool poolA;
    MockPool poolB;
    MockFactory factory;
    address owner = address(0x1);
    address hot = address(0x2);
    address attacker = address(0x3);

    function setUp() public {
        weth = new MockToken();
        token = new MockToken();
        factory = new MockFactory();
        exec = new ArbitrageExecutor(hot, address(weth), address(factory));

        // 池 A：WETH → TOKEN（0.3% fee，1.0002x 回报）
        poolA = new MockPool(weth, token, 3000, 10002);
        // 池 B：TOKEN → WETH（0.3% fee，1.0002x 回报）→ 净回报 1.0004x
        poolB = new MockPool(token, weth, 3000, 10002);
        factory.setPool(address(weth), address(token), address(poolA));
        factory.setPool(address(token), address(weth), address(poolB));

        // 给执行合约 WETH + TOKEN 初始余额
        weth.mint(address(exec), 100e18);
        token.mint(address(exec), 100e18);
        // 池需要初始余额（模拟真实池的储备）
        weth.mint(address(poolA), 1000e18);
        token.mint(address(poolA), 1000e18);
        weth.mint(address(poolB), 1000e18);
        token.mint(address(poolB), 1000e18);
        // 池需要 approve（模拟池从合约拉款）
        vm.startPrank(address(exec));
        weth.approve(address(poolA), type(uint256).max);
        token.approve(address(poolB), type(uint256).max);
        vm.stopPrank();
    }

    function hopsWethToken() internal view returns (ArbitrageExecutor.Hop[] memory) {
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](2);
        hs[0] = ArbitrageExecutor.Hop({pool: address(poolA), tokenIn: address(weth), tokenOut: address(token), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(poolB), tokenIn: address(token), tokenOut: address(weth), fee: 3000});
        return hs;
    }

    function testSuccessfulTwoHopCycle() public {
        uint256 wethBefore = weth.balanceOf(address(exec));
        vm.prank(hot);
        uint256 profit = exec.executeV3Cycle(hopsWethToken(), 1e18, 1, block.timestamp + 100);
        uint256 wethAfter = weth.balanceOf(address(exec));
        // 1e18 * 1.0004x - 1e18 ≈ 4e14
        assertGt(profit, 0, "profit should be positive");
        assertEq(wethAfter - wethBefore, profit, "profit must equal balance diff");
    }

    function testOnlyExecutorCanExecute() public {
        vm.prank(attacker);
        vm.expectRevert();
        exec.executeV3Cycle(hopsWethToken(), 1e18, 1, block.timestamp + 100);
    }

    function testDeadlinePassed() public {
        vm.prank(hot);
        vm.expectRevert(ArbitrageExecutor.DeadlinePassed.selector);
        exec.executeV3Cycle(hopsWethToken(), 1e18, 1, block.timestamp - 1);
    }

    function testMinProfitNotMet() public {
        // 要求 1e16 利润，实际只有 ~4e14 → revert
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(hopsWethToken(), 1e18, 1e16, block.timestamp + 100);
    }

    function testMaliciousCallbackReverts() public {
        // 非白名单地址直接调回调 → revert（msg.sender != 期望池）
        vm.prank(attacker);
        vm.expectRevert();
        exec.uniswapV3SwapCallback(1e18, 0, abi.encode(uint256(0)));
    }

    function testReentrancyReverts() public {
        // 池回调中重入 executeV3Cycle → 被 nonReentrant 拦截
        // 用一个恶意池：回调里再调 execute
        ReentrantPool rp = new ReentrantPool(exec, hot);
        MockToken t0 = new MockToken();
        MockToken t1 = new MockToken();
        factory.setPool(address(t0), address(t1), address(rp));
        rp.setTokens(t0, t1);
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](2);
        hs[0] = ArbitrageExecutor.Hop({pool: address(rp), tokenIn: address(t0), tokenOut: address(t1), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(poolB), tokenIn: address(token), tokenOut: address(weth), fee: 3000});
        // t0/t1 不衔接 weth→token？先构造 weth 开始的：让 t0=weth
        hs[0] = ArbitrageExecutor.Hop({pool: address(rp), tokenIn: address(weth), tokenOut: address(token), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(poolB), tokenIn: address(token), tokenOut: address(weth), fee: 3000});
        rp.setTokens(weth, token);
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(hs, 1e18, 1, block.timestamp + 100);
    }

    function testDuplicatePoolReverts() public {
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](2);
        hs[0] = ArbitrageExecutor.Hop({pool: address(poolA), tokenIn: address(weth), tokenOut: address(token), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(poolA), tokenIn: address(token), tokenOut: address(weth), fee: 3000});
        vm.prank(hot);
        vm.expectRevert();
        exec.executeV3Cycle(hs, 1e18, 1, block.timestamp + 100);
    }
}

// 恶意池：回调中重入执行
contract ReentrantPool {
    ArbitrageExecutor exec;
    address hot;
    MockToken public token0;
    MockToken public token1;

    constructor(ArbitrageExecutor e, address h) {
        exec = e;
        hot = h;
    }

    function setTokens(MockToken t0, MockToken t1) external {
        token0 = t0;
        token1 = t1;
    }

    function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160, bytes calldata data)
        external
        returns (int256, int256)
    {
        // 重入：在回调中再执行
        ArbitrageExecutor.Hop[] memory hs = new ArbitrageExecutor.Hop[](2);
        hs[0] = ArbitrageExecutor.Hop({pool: address(this), tokenIn: address(token0), tokenOut: address(token1), fee: 3000});
        hs[1] = ArbitrageExecutor.Hop({pool: address(this), tokenIn: address(token1), tokenOut: address(token0), fee: 3000});
        // 重入：回调里再次调用 executeV3Cycle（msg.sender=池，会被 onlyExecutor/nonReentrant 拒绝）
        (bool ok,) = address(exec).call(abi.encodeWithSignature(
            "executeV3Cycle((address,address,address,uint24)[],uint256,uint256,uint256)",
            abi.encode(hs), 1e18, 1, block.timestamp + 100));
        require(!ok, "reentrancy should have failed");
        return (0, 0);
    }
}
