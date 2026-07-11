import { useEffect, useRef, useCallback, useState } from "react";
import { wsManager, type WebSocketEventType } from "@/lib/websocket";
import { WS_ENABLED } from "@/lib/config";

// Hook for using WebSocket with cleanup
export function useWebSocket(
  eventType: WebSocketEventType,
  handler: (data: unknown) => void,
  enabled: boolean = true
) {
  const handlerRef = useRef(handler);
  
  // Keep handler ref up to date
  useEffect(() => {
    handlerRef.current = handler;
  }, [handler]);

  useEffect(() => {
    if (!enabled || !WS_ENABLED) return;

    // Connect WebSocket
    wsManager.connect();

    // Subscribe to event
    const unsubscribe = wsManager.on(eventType, (data) => {
      handlerRef.current(data);
    });

    return () => {
      unsubscribe();
    };
  }, [eventType, enabled]);

  const disconnect = useCallback(() => {
    wsManager.disconnect();
  }, []);

  return { disconnect };
}

// Hook for WebSocket connection status
export function useWebSocketStatus() {
  const [isConnected, setIsConnected] = useState(false);
  const [connectionLost, setConnectionLost] = useState(false);

  useEffect(() => {
    if (!WS_ENABLED) {
      setIsConnected(false);
      setConnectionLost(false);
      return;
    }

    // Check connection status periodically
    const checkConnection = () => {
      const connected = wsManager.isConnected();
      setIsConnected(connected);
      if (!connected && !connectionLost) {
        setConnectionLost(true);
      } else if (connected && connectionLost) {
        setConnectionLost(false);
      }
    };

    checkConnection();
    const interval = setInterval(checkConnection, 5000);

    return () => clearInterval(interval);
  }, [connectionLost]);

  return { isConnected, connectionLost };
}
