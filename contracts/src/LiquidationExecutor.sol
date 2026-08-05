// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {BaseExecutor} from "./BaseExecutor.sol";

/// @title LiquidationExecutor
/// @notice Morpho Blue 清算执行合约：flashLoan -> liquidate -> DEX 换回 -> 还贷 -> 检查 minProfit。
/// @dev 安全要求：
///       - 仅授权执行钱包可调用
///       - require(msg.sender == address(MORPHO)) 在 flashLoan 回调内
///       - profit >= minProfit、block.timestamp <= deadline
///       - 回调结束前必须批准 Morpho 取回借款，否则整笔回滚
interface IMorpho {
    struct MarketParams {
        address loanToken;
        address collateralToken;
        address oracle;
        address irm;
        uint256 lltv;
    }
    function flashLoan(address token, uint256 assets, bytes calldata data) external;
    function liquidate(
        MarketParams calldata marketParams,
        address borrower,
        uint256 seizedAssets,
        uint256 repayAssets,
        bytes calldata data
    ) external returns (uint256, uint256);
}

interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
    function transfer(address, uint256) external returns (bool);
}

/// @notice 回调参数解码：市场五元组 + 借款人 + 借款 token 数量 + 兑换路径。
library FlashData {
    struct Params {
        address loanToken;
        address collateralToken;
        address oracle;
        address irm;
        uint256 lltv;
        address borrower;
        uint256 repayAssets;
        address swapRouter;
        address weth;
        uint256 minProfit;
        uint256 deadline;
    }

    function decode(bytes calldata data) internal pure returns (Params memory p) {
        // 简化编码：abi.encode 各字段顺序固定（生产用完整编码器）
        p = abi.decode(data, (Params));
    }
}

contract LiquidationExecutor is BaseExecutor {
    IMorpho public immutable morpho;
    uint256 public constant MAX_SEIZED_RATIO = 0.5e18; // 单次清算最多 50% 仓位（Morpho 限制）

    error CallbackNotFromMorpho();
    error InsufficientProfit(uint256 actual, uint256 min);
    error DeadlinePassed();

    constructor(address executor_, address morpho_) BaseExecutor(executor_) {
        morpho = IMorpho(morpho_);
    }

    /// @notice 发起清算。MVP 只实现 MorphoFlashFunding 一种资金来源。
    function execute(
        address loanToken,
        uint256 flashAmount,
        bytes calldata paramsData
    ) external onlyExecutor whenNotPaused returns (uint256 profit) {
        FlashData.Params memory p = FlashData.decode(paramsData);
        if (block.timestamp > p.deadline) revert DeadlinePassed();
        morpho.flashLoan(loanToken, flashAmount, paramsData);
        // flashLoan 回调内已扣除偿还；这里验证利润
        profit = IERC20(loanToken).balanceOf(address(this));
        if (profit < p.minProfit) revert InsufficientProfit(profit, p.minProfit);
        // 转给执行钱包
        require(IERC20(loanToken).transfer(executor, profit), "LE:transfer-failed");
    }

    /// @dev Morpho 回调：必须校验调用者。
    function onMorphoFlashLoan(
        uint256,
        bytes calldata data
    ) external {
        if (msg.sender != address(morpho)) revert CallbackNotFromMorpho();
        FlashData.Params memory p = FlashData.decode(data);

        // 1. 清算：偿还借款，获得抵押品
        IMorpho.MarketParams memory mp = IMorpho.MarketParams({
            loanToken: p.loanToken,
            collateralToken: p.collateralToken,
            oracle: p.oracle,
            irm: p.irm,
            lltv: p.lltv
        });
        // 授权 Morpho 取走还款
        require(IERC20(p.loanToken).approve(address(morpho), p.repayAssets), "LE:approve-morpho");
        (uint256 repaid, uint256 seized) = morpho.liquidate(mp, p.borrower, 0, p.repayAssets, "");

        // 2. 抵押品通过 DEX 换回借款 token（白名单 Router）
        // MVP 占位：兑换逻辑与 ArbitrageExecutor 共用 Router 白名单。
        // 生产在此调用 whitelistedRouters 中的 Router，将 collateralToken 全部换回 loanToken。
        require(seized > 0 && repaid > 0, "LE:no-liquidation");

        // 3. 批准 Morpho 取回 flashLoan 本金 + 利息
        //    由 Morpho 在回调返回后自动 pull；此处确保 approve 充足
        require(IERC20(p.loanToken).approve(address(morpho), type(uint256).max), "LE:approve-flash");
    }

    receive() external payable {}
}
