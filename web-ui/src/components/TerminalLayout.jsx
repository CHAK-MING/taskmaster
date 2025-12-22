import React from 'react';
import { LayoutDashboard, GitBranch, TerminalSquare, Settings, Bell, Search } from 'lucide-react';

export const TerminalLayout = ({ children, activeTab, onTabChange, health }) => {
    const pageInfo = {
        dashboard: { breadcrumb: '首页 / 仪表盘', title: '仪表盘' },
        dags: { breadcrumb: '首页 / DAG 管理', title: 'DAG 管理' },
        terminal: { breadcrumb: '首页 / 运行日志', title: '运行日志' },
        settings: { breadcrumb: '首页 / 系统设置', title: '系统设置' }
    };
    const current = pageInfo[activeTab] || pageInfo.dashboard;

    return (
        <div className="app-container">
            <aside className="sidebar">
                <div className="sidebar-logo">
                    <div style={{ width: 32, height: 32, borderRadius: 8, background: 'linear-gradient(135deg, #868CFF 0%, #4318FF 100%)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <span style={{ color: 'white', fontWeight: 'bold', fontSize: 14 }}>TM</span>
                    </div>
                    TaskMaster
                </div>

                <div className="sidebar-section-label">菜单</div>

                <div className="sidebar-menu">
                    <NavItem icon={<LayoutDashboard size={20} />} label="仪表盘" isActive={activeTab === 'dashboard'} onClick={() => onTabChange('dashboard')} />
                    <NavItem icon={<GitBranch size={20} />} label="DAG 管理" isActive={activeTab === 'dags'} onClick={() => onTabChange('dags')} />
                    <NavItem icon={<TerminalSquare size={20} />} label="运行日志" isActive={activeTab === 'terminal'} onClick={() => onTabChange('terminal')} />
                    <NavItem icon={<Settings size={20} />} label="系统设置" isActive={activeTab === 'settings'} onClick={() => onTabChange('settings')} />
                </div>

                {/* 系统状态 */}
                <div style={{ marginTop: 'auto' }}>
                    <div style={{ padding: 16, background: 'var(--bg-body)', borderRadius: 12, fontSize: 13 }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                            <div style={{ width: 8, height: 8, borderRadius: '50%', background: health ? 'var(--success)' : 'var(--danger)' }}></div>
                            <span style={{ color: 'var(--text-main)', fontWeight: 500 }}>{health ? '已连接' : '未连接'}</span>
                        </div>
                        <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>API 服务状态</div>
                    </div>
                </div>
            </aside>

            <div className="main-content">
                <header className="top-header">
                    <div className="page-title-section">
                        <div className="breadcrumb">{current.breadcrumb}</div>
                        <div className="current-page-name">{current.title}</div>
                    </div>

                    <div className="header-actions">
                        <div className="search-box">
                            <Search size={18} style={{ color: 'var(--text-muted)' }} />
                            <input placeholder="搜索..." />
                        </div>
                        <button className="icon-btn notification-btn" title="通知">
                            <Bell size={20} />
                        </button>
                        <div className="user-avatar">管</div>
                    </div>
                </header>

                <main className="content-scroll">{children}</main>
            </div>
        </div>
    );
};

const NavItem = ({ icon, label, isActive, onClick }) => (
    <button className={`nav-item ${isActive ? 'active' : ''}`} onClick={onClick}>
        {icon}
        <span>{label}</span>
    </button>
);
