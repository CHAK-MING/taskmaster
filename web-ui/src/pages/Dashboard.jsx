import React, { useEffect, useState } from 'react';
import { api } from '../api';
import { Activity, Layers, Cpu, Clock, CheckCircle, Play, GitBranch, AlertCircle } from 'lucide-react';

export const Dashboard = () => {
    const [status, setStatus] = useState(null);
    const [dags, setDags] = useState([]);
    const [loading, setLoading] = useState(true);
    const [toast, setToast] = useState(null);

    const showToast = (message, type = 'success') => {
        setToast({ message, type });
        setTimeout(() => setToast(null), 3000);
    };

    useEffect(() => {
        const loadData = async () => {
            try {
                const [statusData, dagsData] = await Promise.all([
                    api.getStatus(),
                    api.listDags()
                ]);
                setStatus(statusData);
                setDags(dagsData || []);
            } catch (e) {
                console.warn('加载数据失败', e);
            } finally {
                setLoading(false);
            }
        };
        loadData();
        const interval = setInterval(loadData, 5000);
        return () => clearInterval(interval);
    }, []);

    const totalTasks = dags.reduce((sum, dag) => sum + (dag.task_count || dag.tasks?.length || 0), 0);
    const cronTasks = dags.reduce((sum, dag) => {
        const tasks = dag.tasks || [];
        return sum + tasks.filter(t => t.cron).length;
    }, 0);

    const handleTriggerDag = async (dagId) => {
        try {
            await api.triggerDag(dagId);
            showToast('DAG 已触发运行');
        } catch (e) {
            showToast('触发失败: ' + e.message, 'error');
        }
    };

    return (
        <div>
            {/* 统计卡片 */}
            <div className="grid-4" style={{ marginBottom: 24 }}>
                <StatCard 
                    title="DAG 数量" 
                    value={dags.length} 
                    icon={<GitBranch size={24} />} 
                    sub={`${totalTasks} 个任务`} 
                />
                <StatCard 
                    title="活跃运行" 
                    value={status?.active_runs ?? 0} 
                    icon={<Activity size={24} />} 
                    sub="DAG 执行中" 
                />
                <StatCard 
                    title="定时任务" 
                    value={cronTasks} 
                    icon={<Clock size={24} />} 
                    sub="Cron 调度" 
                />
                <StatCard 
                    title="系统状态" 
                    value={status?.running ? '运行中' : '已停止'} 
                    icon={<Cpu size={24} />} 
                    status={status?.running ? 'success' : 'error'} 
                />
            </div>

            <div className="grid-2-1">
                {/* DAG 列表 */}
                <div className="card">
                    <div className="card-header">
                        <div className="card-title">DAG 列表</div>
                        <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{dags.length} 个 DAG</span>
                    </div>

                    <div style={{ maxHeight: 400, overflowY: 'auto' }}>
                        {dags.map(dag => (
                            <DAGItem key={dag.id} dag={dag} onTrigger={handleTriggerDag} />
                        ))}
                        {dags.length === 0 && (
                            <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
                                {loading ? '加载中...' : '暂无 DAG'}
                            </div>
                        )}
                    </div>
                </div>

                {/* 系统状态 */}
                <div className="card">
                    <div className="card-header">
                        <div className="card-title">系统状态</div>
                    </div>
                    
                    <StatusItem icon={<CheckCircle size={14} />} label="API 服务" status="正常" ok={true} />
                    <StatusItem icon={<Clock size={14} />} label="调度引擎" status={status?.running ? '运行中' : '已停止'} ok={status?.running} />
                    <StatusItem icon={<Activity size={14} />} label="执行器" status="就绪" ok={true} />
                    <StatusItem icon={<GitBranch size={14} />} label="DAG 数量" status={`${dags.length} 个`} ok={true} />
                    <StatusItem icon={<Layers size={14} />} label="任务总数" status={`${totalTasks} 个`} ok={true} />

                    {/* DAG 快速触发 */}
                    {dags.length > 0 && (
                        <div style={{ marginTop: 20, paddingTop: 16, borderTop: '1px solid var(--border-light)' }}>
                            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 12 }}>快速触发</div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                                {dags.slice(0, 3).map(dag => (
                                    <button 
                                        key={dag.id}
                                        className="btn" 
                                        style={{ padding: '6px 12px', fontSize: 11 }}
                                        onClick={() => handleTriggerDag(dag.id)}
                                    >
                                        <Play size={12} /> {dag.name}
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>

            {/* Toast 提示 */}
            {toast && (
                <div className={`toast ${toast.type}`}>
                    {toast.type === 'success' ? <CheckCircle size={16} /> : <AlertCircle size={16} />}
                    {toast.message}
                </div>
            )}
        </div>
    );
};

const StatCard = ({ title, value, icon, sub, status }) => (
    <div className="card" style={{ padding: 20, display: 'flex', alignItems: 'center', gap: 16 }}>
        <div className="stat-icon-wrapper">{icon}</div>
        <div>
            <div style={{ fontSize: 13, color: 'var(--text-muted)' }}>{title}</div>
            <div style={{ fontSize: 22, fontWeight: 'bold', color: status === 'error' ? 'var(--danger)' : status === 'success' ? 'var(--success)' : 'var(--text-main)' }}>{value}</div>
            {sub && <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{sub}</div>}
        </div>
    </div>
);

const DAGItem = ({ dag, onTrigger }) => {
    const taskCount = dag.task_count || dag.tasks?.length || 0;
    return (
        <div style={{ padding: 14, background: 'var(--bg-body)', borderRadius: 10, marginBottom: 8, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{ padding: 8, background: 'var(--primary-light)', borderRadius: 8, color: 'var(--primary)' }}>
                    <GitBranch size={16} />
                </div>
                <div>
                    <div style={{ fontWeight: 500, color: 'var(--text-main)', fontSize: 13 }}>{dag.name}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', display: 'flex', gap: 8, marginTop: 2 }}>
                        <span style={{ fontFamily: 'monospace' }}>{dag.id}</span>
                        <span>· {taskCount} 个任务</span>
                        {dag.from_config && <span>· 只读</span>}
                    </div>
                </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <button className="btn btn-primary" style={{ padding: '6px 12px', fontSize: 11 }} onClick={() => onTrigger(dag.id)}>
                    <Play size={12} /> 运行
                </button>
            </div>
        </div>
    );
};

const TaskItem = ({ task, onTrigger }) => {
    const isRoot = !task.deps || task.deps.length === 0;
    return (
        <div style={{ padding: 14, background: 'var(--bg-body)', borderRadius: 10, marginBottom: 8, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{ padding: 8, background: 'var(--primary-light)', borderRadius: 8, color: 'var(--primary)' }}>
                    {isRoot ? <GitBranch size={16} /> : <Layers size={16} />}
                </div>
                <div>
                    <div style={{ fontWeight: 500, color: 'var(--text-main)', fontSize: 13 }}>{task.name || task.id}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', display: 'flex', gap: 8, marginTop: 2 }}>
                        <span style={{ fontFamily: 'monospace' }}>{task.id}</span>
                        {task.cron && <span>· {task.cron}</span>}
                        {task.deps?.length > 0 && <span>· 依赖: {task.deps.join(', ')}</span>}
                    </div>
                </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span className={`badge ${task.enabled ? 'active' : 'inactive'}`} style={{ fontSize: 11 }}>
                    {task.enabled ? '启用' : '禁用'}
                </span>
                {isRoot && (
                    <button className="btn" style={{ padding: '4px 10px', fontSize: 11 }} onClick={() => onTrigger(task.id)}>
                        <Play size={12} />
                    </button>
                )}
            </div>
        </div>
    );
};

const StatusItem = ({ icon, label, status, ok }) => (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 0', borderBottom: '1px solid var(--border-light)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ color: ok ? 'var(--success)' : 'var(--danger)' }}>{icon}</span>
            <span style={{ color: 'var(--text-main)', fontWeight: 500, fontSize: 13 }}>{label}</span>
        </div>
        <span style={{ fontSize: 11, padding: '3px 8px', borderRadius: 12, background: ok ? 'rgba(5, 205, 153, 0.1)' : 'rgba(227, 26, 26, 0.1)', color: ok ? 'var(--success)' : 'var(--danger)' }}>
            {status}
        </span>
    </div>
);
