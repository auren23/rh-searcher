// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @title BaseExecutor
/// @notice 两个执行合约的公共安全底座：执行钱包授权、白名单、暂停、残留资产提取。
///         设计原则：没有任意外部 call 入口；owner 只用于暂停和提取残留资产。
abstract contract BaseExecutor {
    address public immutable owner;
    address public immutable executor; // 唯一可调用 execute 的 EOA（热钱包）

    mapping(address => bool) public whitelistedRouters;   // Router 白名单
    mapping(address => bool) public whitelistedPools;     // Pool 白名单
    bool public paused;

    event Paused(address by);
    event Unpaused(address by);
    event RouterWhitelisted(address router, bool allowed);
    event PoolWhitelisted(address pool, bool allowed);
    event ResidueWithdrawn(address token, address to, uint256 amount);

    modifier onlyOwner() {
        require(msg.sender == owner, "BE:not-owner");
        _;
    }

    modifier onlyExecutor() {
        require(msg.sender == executor, "BE:not-executor");
        _;
    }

    modifier whenNotPaused() {
        require(!paused, "BE:paused");
        _;
    }

    constructor(address executor_) {
        owner = msg.sender;
        executor = executor_;
    }

    /// @dev 只允许 owner 增删 Router/池白名单。
    function setRouter(address router, bool allowed) external onlyOwner {
        whitelistedRouters[router] = allowed;
        emit RouterWhitelisted(router, allowed);
    }

    function setPool(address pool, bool allowed) external onlyOwner {
        whitelistedPools[pool] = allowed;
        emit PoolWhitelisted(pool, allowed);
    }

    function pause() external onlyOwner {
        paused = true;
        emit Paused(msg.sender);
    }

    function unpause() external onlyOwner {
        paused = false;
        emit Unpaused(msg.sender);
    }

    /// @dev 提取残留资产（owner 专用，例如操作失误留下的代币）。
    function withdrawResidue(address token, address to, uint256 amount) external onlyOwner {
        _safeTransfer(token, to, amount);
        emit ResidueWithdrawn(token, to, amount);
    }

    function _safeTransfer(address token, address to, uint256 amount) internal {
        (bool ok, bytes memory ret) = token.call(abi.encodeWithSignature("transfer(address,uint256)", to, amount));
        require(ok && (ret.length == 0 || abi.decode(ret, (bool))), "BE:transfer-failed");
    }
}
