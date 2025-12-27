type WebSocketEventType = 'task_status_changed' | 'dag_run_completed';

interface WebSocketMessage {
  type: 'event' | 'log' | 'connected';
  event?: WebSocketEventType;
  data?: string;
  timestamp?: string;
}

interface TaskStatusChangedData {
  type: 'task_status_changed';
  dag_id: string;
  run_id: string;
  task_id: string;
  status: string;
  timestamp: number;
}

interface DAGRunCompletedData {
  type: 'dag_run_completed';
  dag_id: string;
  run_id: string;
  status: string;
  timestamp: number;
}

type EventHandler = (data: TaskStatusChangedData | DAGRunCompletedData) => void;

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private handlers: Map<WebSocketEventType, Set<EventHandler>> = new Map();
  private reconnectTimer: number | null = null;
  private url: string;

  constructor() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.url = `${protocol}//${window.location.host}/ws/logs`;
  }

  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN) return;

    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
    };

    this.ws.onmessage = (event) => {
      try {
        const message: WebSocketMessage = JSON.parse(event.data);
        
        if (message.type === 'event' && message.event && message.data) {
          const data = JSON.parse(message.data);
          const handlers = this.handlers.get(message.event);
          if (handlers) {
            handlers.forEach(handler => handler(data));
          }
        }
      } catch {
        // Ignore parse errors
      }
    };

    this.ws.onerror = () => {
      // Connection error, will reconnect
    };

    this.ws.onclose = () => {
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    
    this.reconnectTimer = window.setTimeout(() => {
      this.connect();
    }, 3000);
  }

  on(event: WebSocketEventType, handler: EventHandler): () => void {
    if (!this.handlers.has(event)) {
      this.handlers.set(event, new Set());
    }
    this.handlers.get(event)!.add(handler);

    return () => {
      const handlers = this.handlers.get(event);
      if (handlers) {
        handlers.delete(handler);
      }
    };
  }

  disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    
    this.handlers.clear();
  }
}

export const wsManager = new WebSocketManager();
