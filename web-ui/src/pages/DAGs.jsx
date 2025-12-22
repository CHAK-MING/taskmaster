import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { api } from '../api';
import { Play, GitBranch, Clock, Layers, ArrowRight, ChevronDown, ChevronRight, Plus, Trash2, Edit, X, AlertCircle, Lock, CheckCircle } from 'lucide-react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
  Position,
  Handle
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

export const DAGs = () => {
  const [dags, setDags] = useState([]);
  const [selectedDag, setSelectedDag] = useState(null);
  const [selectedTask, setSelectedTask] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showCreateDag, setShowCreateDag] = useState(false);
  const [showAddTask, setShowAddTask] = useState(false);
  const [toast, setToast] = useState(null);

  const showToast = (message, type = 'success') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  useEffect(() => {
    loadDags();
  }, []);

  const loadDags = async () => {
    try {
      const data = await api.listDags();
      setDags(data || []);
    } catch (e) {
      console.warn('加载 DAG 失败', e);
    } finally {
      setLoading(false);
    }
  };

  const handleTriggerDag = async (dagId) => {
    try {
      await api.triggerDag(dagId);
      showToast('DAG 已触发运行');
    } catch (e) {
      showToast('触发失败: ' + e.message, 'error');
    }
  };

  const handleDeleteDag = async (dagId) => {
    if (!confirm('确定要删除这个 DAG 吗？')) return;
    try {
      await api.deleteDag(dagId);
      await loadDags();
      if (selectedDag === dagId) setSelectedDag(null);
      showToast('DAG 已删除');
    } catch (e) {
      showToast('删除失败: ' + e.message, 'error');
    }
  };

  useEffect(() => {
    if (selectedDag && dags.length > 0 && !dags.find(d => d.id === selectedDag)) {
      setSelectedDag(null);
    }
    if (selectedTask && selectedDag) {
      const dag = dags.find(d => d.id === selectedDag);
      if (dag && dag.tasks && !dag.tasks.find(t => t.id === selectedTask)) {
        setSelectedTask(null);
      }
    }
  }, [dags, selectedDag, selectedTask]);

  const renderContent = () => {
    if (selectedDag) {
      const dag = dags.find(d => d.id === selectedDag);
      if (!dag) return null; // Handled by useEffect

      if (selectedTask) {
        const task = (dag.tasks || []).find(t => t.id === selectedTask);
        if (!task) return null; // Handled by useEffect

        return (
          <TaskDetail
            dag={dag}
            task={task}
            onBack={() => setSelectedTask(null)}
            onRefresh={loadDags}
          />
        );
      }

      return (
        <DAGDetail
          dag={dag}
          onBack={() => setSelectedDag(null)}
          onSelectTask={setSelectedTask}
          onTrigger={handleTriggerDag}
          onRefresh={loadDags}
          showAddTask={showAddTask}
          setShowAddTask={setShowAddTask}
        />
      );
    }

    return (
      <div>
        <div style={styles.header}>
          <p style={{ color: 'var(--text-muted)', margin: 0 }}>
            共 {dags.length} 个 DAG
          </p>
          <button className="btn btn-primary" onClick={() => setShowCreateDag(true)}>
            <Plus size={16} /> 新建 DAG
          </button>
        </div>

        {showCreateDag && (
          <CreateDagModal
            onClose={() => setShowCreateDag(false)}
            onCreated={() => { setShowCreateDag(false); loadDags(); }}
          />
        )}

        {loading ? (
          <div style={styles.loading}>加载中...</div>
        ) : dags.length === 0 ? (
          <div className="card" style={styles.emptyState}>
            <GitBranch size={48} style={{ marginBottom: 16, opacity: 0.3 }} />
            <div style={{ fontSize: 16, marginBottom: 8 }}>暂无 DAG</div>
            <div style={{ color: 'var(--text-muted)', fontSize: 13, marginBottom: 20 }}>
              点击上方按钮创建第一个 DAG
            </div>
          </div>
        ) : (
          <div style={styles.grid}>
            {dags.map(dag => (
              <DAGCard
                key={dag.id}
                dag={dag}
                onSelect={() => setSelectedDag(dag.id)}
                onTrigger={() => handleTriggerDag(dag.id)}
                onDelete={() => handleDeleteDag(dag.id)}
              />
            ))}
          </div>
        )}

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

  return renderContent();
};

const styles = {
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 },
  loading: { textAlign: 'center', padding: 40, color: 'var(--text-muted)' },
  emptyState: { textAlign: 'center', padding: 60 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 }
};


const DAGCard = ({ dag, onSelect, onTrigger, onDelete }) => {
  const rootTasks = dag.tasks?.filter(t => !t.deps || t.deps.length === 0) || [];

  return (
    <div className="card" style={{ cursor: 'pointer' }} onClick={onSelect}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <GitBranch size={18} style={{ color: 'var(--primary)' }} />
            <span style={{ fontWeight: 600, fontSize: 15 }}>{dag.name}</span>
            {dag.from_config && (
              <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: 'var(--text-muted)', background: 'var(--bg-body)', padding: '2px 6px', borderRadius: 4 }}>
                <Lock size={10} /> 只读
              </span>
            )}
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' }}>{dag.id}</div>
        </div>
        <div style={{ display: 'flex', gap: 4 }} onClick={e => e.stopPropagation()}>
          <button className="btn btn-primary" style={{ padding: '6px 10px', fontSize: 11 }} onClick={onTrigger}>
            <Play size={12} /> 运行
          </button>
          {!dag.from_config && (
            <button className="btn btn-outline" style={{ padding: '6px 8px', color: 'var(--danger)' }} onClick={onDelete}>
              <Trash2 size={12} />
            </button>
          )}
        </div>
      </div>

      {dag.description && (
        <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 12 }}>{dag.description}</p>
      )}

      <div style={{ display: 'flex', gap: 16, fontSize: 12, color: 'var(--text-muted)' }}>
        <span><Layers size={12} style={{ marginRight: 4 }} />{dag.task_count || dag.tasks?.length || 0} 个任务</span>
        <span><GitBranch size={12} style={{ marginRight: 4 }} />{rootTasks.length} 个入口</span>
      </div>
    </div>
  );
};

const DAGDetail = ({ dag, onBack, onSelectTask, onTrigger, onRefresh, showAddTask, setShowAddTask }) => {
  const taskMap = Object.fromEntries((dag.tasks || []).map(t => [t.id, t]));
  const rootTasks = (dag.tasks || []).filter(t => !t.deps || t.deps.length === 0);
  const getDownstream = (taskId) => (dag.tasks || []).filter(t => t.deps?.includes(taskId));

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <button onClick={onBack} className="btn btn-outline" style={{ padding: '8px 12px' }}>← 返回</button>
        <div style={{ flex: 1 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <h3 style={{ fontSize: 18, marginBottom: 2 }}>{dag.name}</h3>
            {dag.from_config && (
              <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: 'var(--warning)', background: 'rgba(255,193,7,0.1)', padding: '2px 8px', borderRadius: 4 }}>
                <Lock size={10} /> 配置文件加载（只读）
              </span>
            )}
          </div>
          <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' }}>{dag.id}</span>
        </div>
        <button className="btn btn-primary" onClick={() => onTrigger(dag.id)}>
          <Play size={16} /> 触发运行
        </button>
        {!dag.from_config && (
          <button className="btn btn-outline" onClick={() => setShowAddTask(true)}>
            <Plus size={16} /> 添加任务
          </button>
        )}
      </div>

      {showAddTask && (
        <AddTaskModal
          dag={dag}
          onClose={() => setShowAddTask(false)}
          onAdded={() => { setShowAddTask(false); onRefresh(); }}
        />
      )}

      <div className="card" style={{ marginBottom: 20 }}>
        <div className="card-header">
          <div className="card-title">DAG 结构</div>
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{dag.tasks?.length || 0} 个任务</span>
        </div>
        <DAGGraph dag={dag} onSelectTask={onSelectTask} />
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title">全部任务</div>
        </div>
        <table className="modern-table">
          <thead>
            <tr>
              <th>任务 ID</th>
              <th>名称</th>
              <th>命令</th>
              <th>Cron</th>
              <th>依赖</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            {(dag.tasks || []).map(task => (
              <tr key={task.id} onClick={() => onSelectTask(task.id)} style={{ cursor: 'pointer' }}>
                <td className="task-id">{task.id}</td>
                <td>{task.name || '-'}</td>
                <td>
                  <code style={{ fontSize: 11, background: 'white', padding: '2px 6px', borderRadius: 4 }}>
                    {task.command?.length > 40 ? task.command.slice(0, 40) + '...' : task.command}
                  </code>
                </td>
                <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{task.cron || '-'}</td>
                <td style={{ fontSize: 12 }}>{task.deps?.join(', ') || '-'}</td>
                <td>
                  <span className={`badge ${task.enabled ? 'active' : 'inactive'}`}>
                    {task.enabled ? '启用' : '禁用'}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// Custom node component for React Flow
const TaskNode = ({ data }) => {
  const { task, onSelect } = data;
  const isRoot = !task.deps || task.deps.length === 0;

  return (
    <div
      onClick={() => onSelect(task.id)}
      style={{
        padding: '12px 16px',
        background: task.enabled ? '#ffffff' : '#fafafa',
        border: `2px solid ${task.enabled ? '#e2e8f0' : '#ef4444'}`,
        borderRadius: 12,
        width: 180,
        cursor: 'pointer',
        opacity: task.enabled ? 1 : 0.7,
        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
        transition: 'all 0.2s ease',
        position: 'relative',
      }}
    >
      {/* Left handle for incoming edges */}
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: '#94a3b8', width: 8, height: 8 }}
      />
      {/* Right handle for outgoing edges */}
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: '#6366f1', width: 8, height: 8 }}
      />
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        {isRoot ? (
          <GitBranch size={14} color="#6366f1" />
        ) : (
          <Layers size={14} color="#94a3b8" />
        )}
        <span style={{
          fontWeight: 600,
          fontSize: 13,
          color: '#1e293b',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
          flex: 1
        }}>
          {task.name || task.id}
        </span>
      </div>
      <div style={{ fontSize: 11, color: '#64748b', fontFamily: 'monospace' }}>
        {task.id}
      </div>
      {!task.enabled && (
        <div style={{
          position: 'absolute',
          top: -8,
          right: -8,
          background: '#ef4444',
          color: 'white',
          fontSize: 9,
          padding: '2px 8px',
          borderRadius: 10,
          fontWeight: 600
        }}>
          DISABLED
        </div>
      )}
    </div>
  );
};

const nodeTypes = { taskNode: TaskNode };

const DAGGraph = ({ dag, onSelectTask }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    if (!dag?.tasks || dag.tasks.length === 0) return;

    // Calculate levels using topological sort
    const taskMap = new Map();
    dag.tasks.forEach(t => taskMap.set(t.id, { ...t, level: 0 }));

    let changed = true;
    while (changed) {
      changed = false;
      taskMap.forEach(task => {
        if (task.deps && task.deps.length > 0) {
          const maxDepLevel = Math.max(...task.deps.map(d => taskMap.get(d)?.level || 0));
          if (task.level <= maxDepLevel) {
            task.level = maxDepLevel + 1;
            changed = true;
          }
        }
      });
    }

    // Group tasks by level
    const levels = new Map();
    taskMap.forEach(task => {
      if (!levels.has(task.level)) levels.set(task.level, []);
      levels.get(task.level).push(task);
    });

    // Create nodes with positions
    const NODE_WIDTH = 200;
    const NODE_HEIGHT = 80;
    const LEVEL_GAP = 280;
    const TASK_GAP = 100;

    const newNodes = [];
    levels.forEach((tasks, level) => {
      const levelHeight = tasks.length * TASK_GAP;
      const startY = -levelHeight / 2;

      tasks.forEach((task, index) => {
        newNodes.push({
          id: task.id,
          type: 'taskNode',
          position: {
            x: level * LEVEL_GAP,
            y: startY + index * TASK_GAP
          },
          data: { task, onSelect: onSelectTask },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        });
      });
    });

    // Create edges
    const newEdges = [];
    dag.tasks.forEach(task => {
      task.deps?.forEach(depId => {
        if (taskMap.has(depId)) {
          newEdges.push({
            id: `${depId}-${task.id}`,
            source: depId,
            target: task.id,
            type: 'smoothstep',
            animated: false,
            style: { stroke: '#94a3b8', strokeWidth: 2 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: '#94a3b8',
              width: 20,
              height: 20,
            },
          });
        }
      });
    });

    setNodes(newNodes);
    setEdges(newEdges);
  }, [dag, onSelectTask, setNodes, setEdges]);

  if (!dag?.tasks || dag.tasks.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
        <Layers size={32} style={{ marginBottom: 12, opacity: 0.5 }} />
        <div>暂无任务</div>
        {!dag.from_config && <div style={{ fontSize: 12 }}>点击上方按钮添加任务</div>}
      </div>
    );
  }

  return (
    <div style={{ height: 400, background: '#f8fafc', borderRadius: 12 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnScroll
        zoomOnScroll
        minZoom={0.3}
        maxZoom={1.5}
      >
        <Background color="#e2e8f0" gap={20} />
        <Controls showInteractive={false} />
        <MiniMap
          nodeColor={() => '#6366f1'}
          maskColor="rgba(248, 250, 252, 0.8)"
          style={{ background: '#ffffff', border: '1px solid #e2e8f0' }}
        />
      </ReactFlow>
    </div>
  );
};


const TaskDetail = ({ dag, task, onBack, onRefresh }) => {
  const [editing, setEditing] = useState(false);
  const upstream = task.deps?.map(id => dag.tasks?.find(t => t.id === id)).filter(Boolean) || [];
  const downstream = (dag.tasks || []).filter(t => t.deps?.includes(task.id));

  const handleDelete = async () => {
    if (downstream.length > 0) {
      alert('无法删除：有其他任务依赖此任务');
      return;
    }
    if (!confirm('确定要删除这个任务吗？')) return;
    try {
      await api.deleteTask(dag.id, task.id);
      onRefresh();
      onBack();
    } catch (e) {
      alert('删除失败: ' + e.message);
    }
  };

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <button onClick={onBack} className="btn btn-outline" style={{ padding: '8px 12px' }}>← 返回</button>
        <div style={{ flex: 1 }}>
          <h3 style={{ fontSize: 18, marginBottom: 2 }}>{task.name || task.id}</h3>
          <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' }}>{task.id}</span>
        </div>
        {!dag.from_config && (
          <>
            <button className="btn btn-outline" onClick={() => setEditing(true)}>
              <Edit size={16} /> 编辑
            </button>
            <button className="btn btn-outline" style={{ color: 'var(--danger)' }} onClick={handleDelete}>
              <Trash2 size={16} /> 删除
            </button>
          </>
        )}
      </div>

      {editing && (
        <EditTaskModal
          dag={dag}
          task={task}
          onClose={() => setEditing(false)}
          onSaved={() => { setEditing(false); onRefresh(); }}
        />
      )}

      <div className="grid-2-1">
        <div className="card">
          <div className="card-header">
            <div className="card-title">任务配置</div>
          </div>

          <InfoRow label="任务 ID" value={task.id} mono />
          <InfoRow label="名称" value={task.name || '-'} />
          <InfoRow label="状态" value={task.enabled ? '已启用' : '已禁用'} status={task.enabled ? 'success' : 'error'} />
          <InfoRow label="Cron 表达式" value={task.cron || '无（手动触发）'} mono />
          <InfoRow label="超时时间" value={`${task.timeout || 300} 秒`} />
          <InfoRow label="最大重试" value={`${task.max_retries || 3} 次`} />

          <div style={{ marginTop: 16 }}>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 8 }}>执行命令</div>
            <code style={{ display: 'block', padding: 12, background: '#111c44', color: '#a3aed0', borderRadius: 8, fontSize: 12, whiteSpace: 'pre-wrap' }}>
              {task.command}
            </code>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <div className="card-title">依赖关系</div>
          </div>

          {upstream.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 8 }}>上游任务 ({upstream.length})</div>
              {upstream.map(t => (
                <div key={t.id} style={{ padding: 8, background: 'var(--bg-body)', borderRadius: 6, marginBottom: 4, fontSize: 13 }}>
                  <ArrowRight size={12} style={{ marginRight: 6, color: 'var(--text-muted)' }} />
                  {t.name || t.id}
                </div>
              ))}
            </div>
          )}

          {downstream.length > 0 && (
            <div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 8 }}>下游任务 ({downstream.length})</div>
              {downstream.map(t => (
                <div key={t.id} style={{ padding: 8, background: 'var(--bg-body)', borderRadius: 6, marginBottom: 4, fontSize: 13 }}>
                  {t.name || t.id}
                  <ArrowRight size={12} style={{ marginLeft: 6, color: 'var(--text-muted)' }} />
                </div>
              ))}
            </div>
          )}

          {upstream.length === 0 && downstream.length === 0 && (
            <div style={{ textAlign: 'center', padding: 20, color: 'var(--text-muted)', fontSize: 13 }}>
              无依赖关系
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const InfoRow = ({ label, value, mono, status }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 0', borderBottom: '1px solid var(--border-light)' }}>
    <span style={{ color: 'var(--text-muted)', fontSize: 13 }}>{label}</span>
    <span style={{
      color: status === 'success' ? 'var(--success)' : status === 'error' ? 'var(--danger)' : 'var(--text-main)',
      fontWeight: 500,
      fontSize: 13,
      fontFamily: mono ? 'monospace' : 'inherit'
    }}>
      {value}
    </span>
  </div>
);

// ========== Modals ==========

const CreateDagModal = ({ onClose, onCreated }) => {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!name.trim()) {
      setError('请输入 DAG 名称');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await api.createDag(name.trim(), description.trim());
      onCreated();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal title="新建 DAG" onClose={onClose}>
      <form onSubmit={handleSubmit}>
        {error && <div className="error-msg"><AlertCircle size={14} /> {error}</div>}
        <div className="form-group">
          <label>DAG 名称 *</label>
          <input type="text" value={name} onChange={e => setName(e.target.value)} placeholder="例如：数据处理流水线" autoFocus />
        </div>
        <div className="form-group">
          <label>描述</label>
          <textarea value={description} onChange={e => setDescription(e.target.value)} placeholder="可选的描述信息" rows={3} />
        </div>
        <div className="form-actions">
          <button type="button" className="btn btn-outline" onClick={onClose}>取消</button>
          <button type="submit" className="btn btn-primary" disabled={loading}>
            {loading ? '创建中...' : '创建'}
          </button>
        </div>
      </form>
    </Modal>
  );
};

const AddTaskModal = ({ dag, onClose, onAdded }) => {
  const [form, setForm] = useState({
    id: '',
    name: '',
    command: '',
    cron: '',
    deps: [],
    timeout: 300,
    max_retries: 3,
    enabled: true
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.id.trim()) {
      setError('请输入任务 ID');
      return;
    }
    if (!form.command.trim()) {
      setError('请输入执行命令');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await api.addTask(dag.id, {
        ...form,
        id: form.id.trim(),
        name: form.name.trim() || form.id.trim(),
        command: form.command.trim()
      });
      onAdded();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const existingTasks = dag.tasks || [];

  return (
    <Modal title="添加任务" onClose={onClose}>
      <form onSubmit={handleSubmit}>
        {error && <div className="error-msg"><AlertCircle size={14} /> {error}</div>}
        <div className="form-row">
          <div className="form-group">
            <label>任务 ID *</label>
            <input type="text" value={form.id} onChange={e => setForm({ ...form, id: e.target.value })} placeholder="唯一标识，如 extract" />
          </div>
          <div className="form-group">
            <label>任务名称</label>
            <input type="text" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} placeholder="可读名称" />
          </div>
        </div>
        <div className="form-group">
          <label>执行命令 *</label>
          <textarea value={form.command} onChange={e => setForm({ ...form, command: e.target.value })} placeholder="例如：python extract.py" rows={2} />
        </div>
        <div className="form-row">
          <div className="form-group">
            <label>Cron 表达式</label>
            <input type="text" value={form.cron} onChange={e => setForm({ ...form, cron: e.target.value })} placeholder="如 0 * * * *" />
          </div>
          <div className="form-group">
            <label>超时（秒）</label>
            <input type="number" value={form.timeout} onChange={e => setForm({ ...form, timeout: parseInt(e.target.value) || 300 })} />
          </div>
        </div>
        {existingTasks.length > 0 && (
          <div className="form-group">
            <label>依赖任务</label>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {existingTasks.map(t => (
                <label key={t.id} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 13, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={form.deps.includes(t.id)}
                    onChange={e => {
                      if (e.target.checked) {
                        setForm({ ...form, deps: [...form.deps, t.id] });
                      } else {
                        setForm({ ...form, deps: form.deps.filter(d => d !== t.id) });
                      }
                    }}
                  />
                  {t.name || t.id}
                </label>
              ))}
            </div>
          </div>
        )}
        <div className="form-actions">
          <button type="button" className="btn btn-outline" onClick={onClose}>取消</button>
          <button type="submit" className="btn btn-primary" disabled={loading}>
            {loading ? '添加中...' : '添加任务'}
          </button>
        </div>
      </form>
    </Modal>
  );
};

const EditTaskModal = ({ dag, task, onClose, onSaved }) => {
  const [form, setForm] = useState({
    name: task.name || '',
    command: task.command || '',
    cron: task.cron || '',
    deps: task.deps || [],
    timeout: task.timeout || 300,
    max_retries: task.max_retries || 3,
    enabled: task.enabled !== false
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.command.trim()) {
      setError('请输入执行命令');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await api.updateTask(dag.id, task.id, {
        ...form,
        id: task.id,
        name: form.name.trim() || task.id,
        command: form.command.trim()
      });
      onSaved();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const otherTasks = (dag.tasks || []).filter(t => t.id !== task.id);

  return (
    <Modal title={`编辑任务: ${task.id}`} onClose={onClose}>
      <form onSubmit={handleSubmit}>
        {error && <div className="error-msg"><AlertCircle size={14} /> {error}</div>}
        <div className="form-group">
          <label>任务名称</label>
          <input type="text" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
        </div>
        <div className="form-group">
          <label>执行命令 *</label>
          <textarea value={form.command} onChange={e => setForm({ ...form, command: e.target.value })} rows={2} />
        </div>
        <div className="form-row">
          <div className="form-group">
            <label>Cron 表达式</label>
            <input type="text" value={form.cron} onChange={e => setForm({ ...form, cron: e.target.value })} />
          </div>
          <div className="form-group">
            <label>超时（秒）</label>
            <input type="number" value={form.timeout} onChange={e => setForm({ ...form, timeout: parseInt(e.target.value) || 300 })} />
          </div>
        </div>
        {otherTasks.length > 0 && (
          <div className="form-group">
            <label>依赖任务</label>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {otherTasks.map(t => (
                <label key={t.id} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 13, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={form.deps.includes(t.id)}
                    onChange={e => {
                      if (e.target.checked) {
                        setForm({ ...form, deps: [...form.deps, t.id] });
                      } else {
                        setForm({ ...form, deps: form.deps.filter(d => d !== t.id) });
                      }
                    }}
                  />
                  {t.name || t.id}
                </label>
              ))}
            </div>
          </div>
        )}
        <div className="form-group" style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <input
            type="checkbox"
            id="task-enabled"
            checked={form.enabled}
            onChange={e => setForm({ ...form, enabled: e.target.checked })}
            style={{ width: 18, height: 18, cursor: 'pointer' }}
          />
          <label htmlFor="task-enabled" style={{ cursor: 'pointer', userSelect: 'none' }}>
            启用任务
          </label>
        </div>
        <div className="form-actions">
          <button type="button" className="btn btn-outline" onClick={onClose}>取消</button>
          <button type="submit" className="btn btn-primary" disabled={loading}>
            {loading ? '保存中...' : '保存'}
          </button>
        </div>
      </form>
    </Modal>
  );
};

const Modal = ({ title, children, onClose }) => (
  <div className="modal-overlay" onClick={onClose}>
    <div className="modal-content" onClick={e => e.stopPropagation()}>
      <div className="modal-header">
        <h3>{title}</h3>
        <button className="modal-close" onClick={onClose}><X size={20} /></button>
      </div>
      <div className="modal-body">
        {children}
      </div>
    </div>
  </div>
);
