// API 基础路径
const API_BASE = '/api';

export const api = {
  // 健康检查
  getHealth: async () => {
    try {
      const res = await fetch(`${API_BASE}/health`);
      return res.ok;
    } catch {
      return false;
    }
  },
  
  // 获取系统状态
  getStatus: async () => {
    try {
      const res = await fetch(`${API_BASE}/status`);
      if (!res.ok) throw new Error('获取状态失败');
      return res.json();
    } catch (e) {
      console.warn('获取状态失败:', e);
      return null;
    }
  },

  // ========== 旧版任务 API (兼容) ==========
  
  getTasks: async () => {
    try {
      const res = await fetch(`${API_BASE}/tasks`);
      if (!res.ok) throw new Error('获取任务列表失败');
      return res.json();
    } catch (e) {
      console.warn('获取任务列表失败:', e);
      return [];
    }
  },

  getTask: async (id) => {
    try {
      const res = await fetch(`${API_BASE}/tasks/${id}`);
      if (!res.ok) throw new Error('获取任务详情失败');
      return res.json();
    } catch (e) {
      console.warn('获取任务详情失败:', e);
      return null;
    }
  },

  triggerTask: async (taskId) => {
    try {
      const res = await fetch(`${API_BASE}/trigger/${taskId}`, { 
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      if (!res.ok) {
        const error = await res.json();
        throw new Error(error?.error?.message || '触发失败');
      }
      return res.json();
    } catch (e) {
      console.error('触发任务失败:', e);
      throw e;
    }
  },

  // ========== DAG 管理 API ==========

  // 获取所有 DAG
  listDags: async () => {
    try {
      const res = await fetch(`${API_BASE}/dags`);
      if (!res.ok) throw new Error('获取 DAG 列表失败');
      return res.json();
    } catch (e) {
      console.warn('获取 DAG 列表失败:', e);
      return [];
    }
  },

  // 创建新 DAG
  createDag: async (name, description = '') => {
    const res = await fetch(`${API_BASE}/dags`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, description })
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '创建 DAG 失败');
    }
    return res.json();
  },

  // 获取 DAG 详情
  getDag: async (dagId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}`);
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || 'DAG 不存在');
    }
    return res.json();
  },

  // 更新 DAG
  updateDag: async (dagId, name, description) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, description })
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '更新 DAG 失败');
    }
    return res.json();
  },

  // 删除 DAG
  deleteDag: async (dagId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}`, {
      method: 'DELETE'
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '删除 DAG 失败');
    }
    return res.json();
  },

  // 触发 DAG 运行
  triggerDag: async (dagId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '触发 DAG 失败');
    }
    return res.json();
  },

  // ========== 任务管理 API (DAG 内) ==========

  // 获取 DAG 内的任务列表
  listDagTasks: async (dagId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/tasks`);
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '获取任务列表失败');
    }
    return res.json();
  },

  // 添加任务到 DAG
  addTask: async (dagId, task) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/tasks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(task)
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '添加任务失败');
    }
    return res.json();
  },

  // 获取任务详情
  getDagTask: async (dagId, taskId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`);
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '任务不存在');
    }
    return res.json();
  },

  // 更新任务
  updateTask: async (dagId, taskId, task) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(task)
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '更新任务失败');
    }
    return res.json();
  },

  // 删除任务
  deleteTask: async (dagId, taskId) => {
    const res = await fetch(`${API_BASE}/dags/${dagId}/tasks/${taskId}`, {
      method: 'DELETE'
    });
    if (!res.ok) {
      const error = await res.json();
      throw new Error(error?.error?.message || '删除任务失败');
    }
    return res.json();
  }
};

// WebSocket 日志流
export class LogStream {
  constructor(onLog, onOpen, onClose) {
    this.ws = null;
    this.onLog = onLog;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
  }

  connect() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.hostname;
    const port = import.meta.env.DEV ? '8080' : window.location.port;
    const url = `${protocol}//${host}:${port}/ws/logs`;

    try {
      this.ws = new WebSocket(url);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        if (this.onOpen) this.onOpen();
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (this.onLog) this.onLog(data);
        } catch {
          if (this.onLog) {
            this.onLog({ timestamp: new Date().toISOString(), content: event.data });
          }
        }
      };

      this.ws.onclose = () => {
        if (this.onClose) this.onClose();
        if (this.reconnectAttempts < 10) {
          this.reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
          this.reconnectTimer = setTimeout(() => this.connect(), delay);
        }
      };

      this.ws.onerror = (error) => console.warn('WebSocket 错误:', error);
    } catch (e) {
      console.error('WebSocket 连接失败:', e);
    }
  }

  disconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.ws) this.ws.close();
  }
}
