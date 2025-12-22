import React, { useState, useEffect, useRef, useMemo } from 'react';
import { TerminalLayout } from './components/TerminalLayout';
import { Dashboard } from './pages/Dashboard';
import { DAGs } from './pages/DAGs';
import { api, LogStream } from './api';
import { Trash2, Server, Database, Clock, Search, Filter, CheckCircle, XCircle, AlertTriangle, Info, Wifi, Terminal, Play, Square } from 'lucide-react';

function App() {
  const [view, setView] = useState('dashboard');
  const [health, setHealth] = useState(false);
  const [logs, setLogs] = useState([]);
  const logStreamRef = useRef(null);
  const terminalBodyRef = useRef(null);

  useEffect(() => {
    const checkHealth = async () => {
      const isHealthy = await api.getHealth();
      setHealth(isHealthy);
    };
    checkHealth();
    const interval = setInterval(checkHealth, 5000);

    logStreamRef.current = new LogStream(
      (log) => {
        // 处理 WebSocket 消息格式
        let content = log.content || '';
        let taskId = log.task_id || '';
        let runId = log.run_id || '';
        
        // 如果有任务信息，添加前缀
        if (taskId && !content.startsWith('[')) {
          content = `[${taskId}] ${content}`;
        }
        
        setLogs(prev => {
          // 去重：检查最后一条日志是否相同
          const lastLog = prev[prev.length - 1];
          if (lastLog && lastLog.content === content && 
              Math.abs(new Date(lastLog.timestamp) - new Date(log.timestamp)) < 1000) {
            return prev;
          }
          return [...prev.slice(-499), { ...log, content }];
        });
      },
      () => setLogs(prev => [...prev, { timestamp: new Date().toISOString(), content: '[系统] WebSocket 已连接', type: 'system' }]),
      () => setLogs(prev => [...prev, { timestamp: new Date().toISOString(), content: '[系统] WebSocket 已断开', type: 'system' }])
    );
    logStreamRef.current.connect();

    return () => {
      clearInterval(interval);
      if (logStreamRef.current) logStreamRef.current.disconnect();
    };
  }, []);

  useEffect(() => {
    if (terminalBodyRef.current) {
      terminalBodyRef.current.scrollTop = terminalBodyRef.current.scrollHeight;
    }
  }, [logs]);

  const getLogType = (log) => {
    const content = (log.content || '').toLowerCase();
    const stream = log.stream || '';
    
    if (log.type === 'system') return 'system';
    if (stream === 'stderr' || content.includes('error') || content.includes('fail')) return 'error';
    if (content.includes('warn')) return 'warn';
    if (content.includes('success') || content.includes('completed')) return 'success';
    if (content.includes('[info]') || content.includes('started')) return 'info';
    return 'default';
  };

  const getLogIcon = (type) => {
    switch (type) {
      case 'error': return <XCircle size={14} />;
      case 'warn': return <AlertTriangle size={14} />;
      case 'success': return <CheckCircle size={14} />;
      case 'system': return <Wifi size={14} />;
      case 'info': return <Info size={14} />;
      default: return <Terminal size={14} />;
    }
  };

  const formatLogTime = (timestamp) => {
    try {
      return new Date(timestamp).toLocaleTimeString('zh-CN', { hour12: false });
    } catch {
      return '--:--:--';
    }
  };

  // Extract task ID and level tag from log content
  const parseLogContent = (content) => {
    // Pattern: [taskId] [LEVEL] message or [taskId] message or [LEVEL] message
    let taskId = null;
    let levelTag = null;
    let message = content;

    // First, try to extract task ID (usually first bracket)
    const taskMatch = message.match(/^\[([^\]]+)\]\s*/);
    if (taskMatch) {
      const tag = taskMatch[1].toUpperCase();
      // Check if it's a level tag or task ID
      if (['INFO', 'SUCCESS', 'ERROR', 'WARN', 'WARNING', 'DEBUG', '系统'].includes(tag)) {
        levelTag = taskMatch[1];
      } else {
        taskId = taskMatch[1];
      }
      message = message.slice(taskMatch[0].length);
    }

    // Then try to extract level tag from remaining message
    const levelMatch = message.match(/^\[([^\]]+)\]\s*/);
    if (levelMatch) {
      const tag = levelMatch[1].toUpperCase();
      if (['INFO', 'SUCCESS', 'ERROR', 'WARN', 'WARNING', 'DEBUG', 'COMPLETED', 'STARTED', 'FAILED'].includes(tag) || 
          tag.includes('SUCCESS') || tag.includes('INFO') || tag.includes('ERROR')) {
        levelTag = levelMatch[1];
        message = message.slice(levelMatch[0].length);
      }
    }

    return { taskId, levelTag, message };
  };

  // Get color for level tag
  const getLevelTagColor = (tag) => {
    if (!tag) return null;
    const upper = tag.toUpperCase();
    if (upper.includes('SUCCESS') || upper.includes('COMPLETED')) {
      return { bg: 'rgba(22, 163, 74, 0.15)', color: '#16a34a' };
    }
    if (upper.includes('ERROR') || upper.includes('FAIL')) {
      return { bg: 'rgba(220, 38, 38, 0.15)', color: '#dc2626' };
    }
    if (upper.includes('WARN')) {
      return { bg: 'rgba(217, 119, 6, 0.15)', color: '#d97706' };
    }
    if (upper.includes('INFO') || upper.includes('STARTED')) {
      return { bg: 'rgba(79, 70, 229, 0.15)', color: '#4f46e5' };
    }
    if (upper === '系统') {
      return { bg: 'rgba(37, 99, 235, 0.15)', color: '#2563eb' };
    }
    return { bg: 'rgba(100, 116, 139, 0.15)', color: '#64748b' };
  };

  return (
    <TerminalLayout activeTab={view} onTabChange={setView} health={health}>
      {view === 'dashboard' && <Dashboard />}
      {view === 'dags' && <DAGs />}
      {view === 'terminal' && <LogViewer logs={logs} setLogs={setLogs} terminalBodyRef={terminalBodyRef} getLogType={getLogType} getLogIcon={getLogIcon} formatLogTime={formatLogTime} parseLogContent={parseLogContent} getLevelTagColor={getLevelTagColor} />}
      {view === 'settings' && <SettingsView health={health} />}
    </TerminalLayout>
  );
}

const SettingsView = ({ health }) => (
  <div>
    <p style={{ color: 'var(--text-muted)', marginBottom: 24 }}>系统状态信息（只读）</p>
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 20 }}>
      <SettingsCard icon={<Server size={20} />} title="API 服务器" description="REST API 配置">
        <SettingItem label="监听地址" value="0.0.0.0:8080" />
        <SettingItem label="状态" value={health ? '运行中' : '已停止'} status={health ? 'success' : 'error'} />
      </SettingsCard>
      <SettingsCard icon={<Database size={20} />} title="数据持久化" description="SQLite 数据库">
        <SettingItem label="数据库" value="./taskmaster.db" />
        <SettingItem label="状态" value={health ? '已连接' : '未连接'} status={health ? 'success' : 'error'} />
      </SettingsCard>
      <SettingsCard icon={<Clock size={20} />} title="调度器" description="任务调度配置">
        <SettingItem label="默认超时" value="300 秒" />
        <SettingItem label="重试次数" value="3" />
      </SettingsCard>
    </div>
  </div>
);

const SettingsCard = ({ icon, title, description, children }) => (
  <div className="card" style={{ marginBottom: 20 }}>
    <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
      <div style={{ padding: 10, background: 'var(--primary-light)', borderRadius: 10, color: 'var(--primary)' }}>{icon}</div>
      <div>
        <div style={{ fontWeight: 600, color: 'var(--text-main)' }}>{title}</div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>{description}</div>
      </div>
    </div>
    {children}
  </div>
);

const SettingItem = ({ label, value, status }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: '1px solid var(--border-light)' }}>
    <span style={{ color: 'var(--text-muted)', fontSize: 13 }}>{label}</span>
    <span style={{ fontWeight: 500, fontSize: 13, color: status === 'success' ? 'var(--success)' : status === 'error' ? 'var(--danger)' : 'var(--text-main)' }}>{value}</span>
  </div>
);

// Log Viewer Component
const LogViewer = ({ logs, setLogs, terminalBodyRef, getLogType, getLogIcon, formatLogTime, parseLogContent, getLevelTagColor }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('all');
  const [autoScroll, setAutoScroll] = useState(true);

  const filteredLogs = useMemo(() => {
    return logs.filter(log => {
      const type = getLogType(log);
      const matchesFilter = filterType === 'all' || type === filterType;
      const matchesSearch = !searchTerm || 
        (log.content || '').toLowerCase().includes(searchTerm.toLowerCase());
      return matchesFilter && matchesSearch;
    });
  }, [logs, filterType, searchTerm, getLogType]);

  useEffect(() => {
    if (autoScroll && terminalBodyRef.current) {
      terminalBodyRef.current.scrollTop = terminalBodyRef.current.scrollHeight;
    }
  }, [filteredLogs, autoScroll, terminalBodyRef]);

  const logStats = useMemo(() => {
    const stats = { total: logs.length, error: 0, warn: 0, success: 0 };
    logs.forEach(log => {
      const type = getLogType(log);
      if (type === 'error') stats.error++;
      else if (type === 'warn') stats.warn++;
      else if (type === 'success') stats.success++;
    });
    return stats;
  }, [logs, getLogType]);

  const typeColors = {
    error: { bg: 'rgba(239, 68, 68, 0.08)', color: '#dc2626', border: '#dc2626' },
    warn: { bg: 'rgba(245, 158, 11, 0.08)', color: '#d97706', border: '#d97706' },
    success: { bg: 'rgba(22, 163, 74, 0.08)', color: '#16a34a', border: '#16a34a' },
    system: { bg: 'rgba(59, 130, 246, 0.08)', color: '#2563eb', border: '#2563eb' },
    info: { bg: 'rgba(79, 70, 229, 0.08)', color: '#4f46e5', border: '#4f46e5' },
    default: { bg: 'transparent', color: '#1e293b', border: '#e2e8f0' }
  };

  return (
    <div style={{ height: 'calc(100vh - 160px)', display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Stats Bar */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        <div className="card" style={{ padding: '12px 20px', display: 'flex', alignItems: 'center', gap: 12, flex: 1, minWidth: 120 }}>
          <Terminal size={18} style={{ color: 'var(--primary)' }} />
          <div>
            <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--text-main)' }}>{logStats.total}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>总日志</div>
          </div>
        </div>
        <div className="card" style={{ padding: '12px 20px', display: 'flex', alignItems: 'center', gap: 12, flex: 1, minWidth: 120 }}>
          <CheckCircle size={18} style={{ color: '#22c55e' }} />
          <div>
            <div style={{ fontSize: 20, fontWeight: 700, color: '#22c55e' }}>{logStats.success}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>成功</div>
          </div>
        </div>
        <div className="card" style={{ padding: '12px 20px', display: 'flex', alignItems: 'center', gap: 12, flex: 1, minWidth: 120 }}>
          <AlertTriangle size={18} style={{ color: '#f59e0b' }} />
          <div>
            <div style={{ fontSize: 20, fontWeight: 700, color: '#f59e0b' }}>{logStats.warn}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>警告</div>
          </div>
        </div>
        <div className="card" style={{ padding: '12px 20px', display: 'flex', alignItems: 'center', gap: 12, flex: 1, minWidth: 120 }}>
          <XCircle size={18} style={{ color: '#ef4444' }} />
          <div>
            <div style={{ fontSize: 20, fontWeight: 700, color: '#ef4444' }}>{logStats.error}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>错误</div>
          </div>
        </div>
      </div>

      {/* Terminal Window */}
      <div className="card" style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: 0, overflow: 'hidden' }}>
        {/* Header */}
        <div style={{ 
          padding: '12px 20px', 
          background: 'var(--bg-body)',
          borderBottom: '1px solid var(--border-light)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <Terminal size={18} style={{ color: 'var(--primary)' }} />
            <div style={{ fontSize: 14, color: 'var(--text-main)', fontWeight: 600 }}>实时日志流</div>
          </div>
          
          {/* Search & Filter */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{ position: 'relative' }}>
              <Search size={14} style={{ position: 'absolute', left: 10, top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
              <input
                type="text"
                placeholder="搜索日志..."
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                style={{
                  background: 'var(--bg-body)',
                  border: '1px solid var(--border-light)',
                  borderRadius: 8,
                  padding: '8px 12px 8px 32px',
                  color: 'var(--text-main)',
                  fontSize: 12,
                  width: 160,
                  outline: 'none'
                }}
              />
            </div>
            
            <select
              value={filterType}
              onChange={e => setFilterType(e.target.value)}
              style={{
                background: 'var(--bg-body)',
                border: '1px solid var(--border-light)',
                borderRadius: 8,
                padding: '8px 12px',
                color: 'var(--text-main)',
                fontSize: 12,
                outline: 'none',
                cursor: 'pointer'
              }}
            >
              <option value="all">全部类型</option>
              <option value="success">✓ 成功</option>
              <option value="error">✗ 错误</option>
              <option value="warn">⚠ 警告</option>
              <option value="info">ℹ 信息</option>
              <option value="system">⚡ 系统</option>
            </select>

            <button
              onClick={() => setAutoScroll(!autoScroll)}
              style={{
                background: autoScroll ? 'rgba(22, 163, 74, 0.1)' : 'var(--bg-body)',
                border: `1px solid ${autoScroll ? '#16a34a' : 'var(--border-light)'}`,
                borderRadius: 8,
                padding: '8px 12px',
                color: autoScroll ? '#16a34a' : 'var(--text-muted)',
                fontSize: 12,
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 6
              }}
              title={autoScroll ? '自动滚动已开启' : '自动滚动已关闭'}
            >
              {autoScroll ? <Play size={12} /> : <Square size={12} />}
              自动滚动
            </button>
            
            <button
              onClick={() => setLogs([])}
              style={{
                background: 'rgba(220, 38, 38, 0.1)',
                border: '1px solid rgba(220, 38, 38, 0.3)',
                borderRadius: 8,
                padding: '8px 12px',
                color: '#dc2626',
                fontSize: 12,
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: 6
              }}
            >
              <Trash2 size={12} /> 清空
            </button>
          </div>
        </div>

        {/* Log Body */}
        <div 
          ref={terminalBodyRef}
          style={{ 
            flex: 1, 
            overflowY: 'auto',
            background: '#ffffff'
          }}
        >
          {filteredLogs.length === 0 ? (
            <div style={{ 
              display: 'flex', 
              flexDirection: 'column', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: '100%',
              color: 'var(--text-muted)',
              gap: 12
            }}>
              <Terminal size={40} style={{ opacity: 0.3 }} />
              <div style={{ fontSize: 14 }}>
                {logs.length === 0 ? '等待日志...' : '没有匹配的日志'}
              </div>
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm('')}
                  style={{
                    background: 'var(--bg-body)',
                    border: '1px solid var(--border-light)',
                    borderRadius: 6,
                    padding: '6px 12px',
                    color: 'var(--text-muted)',
                    fontSize: 12,
                    cursor: 'pointer'
                  }}
                >
                  清除搜索
                </button>
              )}
            </div>
          ) : (
            filteredLogs.map((log, i) => {
              const type = getLogType(log);
              const colors = typeColors[type];
              const { taskId, levelTag, message } = parseLogContent(log.content || '');
              const levelColors = getLevelTagColor(levelTag);
              
              return (
                <div
                  key={i}
                  style={{
                    display: 'flex',
                    alignItems: 'flex-start',
                    gap: 10,
                    padding: '10px 20px',
                    borderLeft: `3px solid ${colors.border}`,
                    background: i % 2 === 0 ? '#ffffff' : '#fafbfc',
                    transition: 'background 0.15s'
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = '#f1f5f9'}
                  onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? '#ffffff' : '#fafbfc'}
                >
                  {/* Time */}
                  <span style={{ 
                    fontSize: 11, 
                    color: '#64748b', 
                    fontFamily: 'monospace',
                    minWidth: 70,
                    flexShrink: 0
                  }}>
                    {formatLogTime(log.timestamp)}
                  </span>
                  
                  {/* Icon */}
                  <span style={{ color: colors.color, flexShrink: 0, marginTop: 1 }}>
                    {getLogIcon(type)}
                  </span>
                  
                  {/* Task ID Badge */}
                  {taskId && (
                    <span style={{
                      background: 'rgba(100, 116, 139, 0.12)',
                      color: '#475569',
                      padding: '2px 8px',
                      borderRadius: 4,
                      fontSize: 11,
                      fontFamily: 'monospace',
                      fontWeight: 600,
                      flexShrink: 0
                    }}>
                      {taskId}
                    </span>
                  )}

                  {/* Level Tag Badge */}
                  {levelTag && levelColors && (
                    <span style={{
                      background: levelColors.bg,
                      color: levelColors.color,
                      padding: '2px 8px',
                      borderRadius: 4,
                      fontSize: 11,
                      fontFamily: 'monospace',
                      fontWeight: 700,
                      flexShrink: 0,
                      textTransform: 'uppercase'
                    }}>
                      {levelTag}
                    </span>
                  )}
                  
                  {/* Message */}
                  <span style={{ 
                    color: colors.color,
                    fontSize: 13,
                    fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                    wordBreak: 'break-all',
                    flex: 1,
                    lineHeight: 1.6
                  }}>
                    {message}
                  </span>
                </div>
              );
            })
          )}
        </div>

        {/* Footer Status */}
        <div style={{
          padding: '10px 20px',
          borderTop: '1px solid var(--border-light)',
          background: 'var(--bg-body)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          fontSize: 12,
          color: 'var(--text-muted)'
        }}>
          <span>显示 {filteredLogs.length} / {logs.length} 条日志</span>
          <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ 
              width: 8, 
              height: 8, 
              borderRadius: '50%', 
              background: '#16a34a',
              boxShadow: '0 0 8px rgba(22, 163, 74, 0.5)'
            }}></span>
            实时连接中
          </span>
        </div>
      </div>
    </div>
  );
};

export default App;
