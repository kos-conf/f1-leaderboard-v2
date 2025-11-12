import React, { useState, useEffect, useRef } from 'react';
import { useEventSource } from '../hooks/useEventSource';

const AnomalyPanel = ({ raceId }) => {
  const [anomalies, setAnomalies] = useState([]);
  const [highlightedAnomaly, setHighlightedAnomaly] = useState(null);
  const [featureEnabled, setFeatureEnabled] = useState(true);
  const anomalyEndRef = useRef(null);
  
  // Build URL with optional race_id query parameter
  const anomalyUrl = raceId 
    ? `http://localhost:8001/api/anomalies/stream?race_id=${raceId}`
    : 'http://localhost:8001/api/anomalies/stream';
  
  const { data: anomalyData, error: anomalyError, isConnected: anomalyConnected } = useEventSource(anomalyUrl);

  // Auto-scroll to top when new anomaly arrives
  const scrollToTop = () => {
    if (anomalyEndRef.current) {
      anomalyEndRef.current.scrollTop = 0;
    }
  };

  useEffect(() => {
    scrollToTop();
  }, [anomalies]);

  // Handle new anomaly data
  useEffect(() => {
    if (anomalyData?.type === 'anomaly') {
      const newAnomaly = anomalyData.data;
      
      setAnomalies(prev => {
        // Check for duplicates by timestamp, team_name, and metric_name combination
        // Use a more lenient check - only consider duplicate if same timestamp AND same team AND same metric
        const isDuplicate = prev.some(anomaly => 
          anomaly.timestamp === newAnomaly.timestamp &&
          anomaly.team_name === newAnomaly.team_name &&
          anomaly.metric_name === newAnomaly.metric_name
        );
        if (isDuplicate) {
          return prev;
        }
        
        // Add new anomaly at the top and keep only last 50
        return [newAnomaly, ...prev].slice(0, 50);
      });
      
      // Highlight the new anomaly using a unique identifier
      const highlightId = `${newAnomaly.timestamp}-${newAnomaly.team_name}-${newAnomaly.metric_name}`;
      setHighlightedAnomaly(highlightId);
      
      // Remove highlight after 5 seconds
      setTimeout(() => {
        setHighlightedAnomaly(null);
      }, 5000);
    }
  }, [anomalyData]);

  // Check if feature is enabled on mount
  useEffect(() => {
    const checkFeature = async () => {
      try {
        const response = await fetch('http://localhost:8001/api/health');
        if (response.ok) {
          const data = await response.json();
          setFeatureEnabled(data.features?.anomaly_detection || false);
        }
      } catch (error) {
        console.error('Error checking feature status:', error);
        setFeatureEnabled(false);
      }
    };
    checkFeature();
  }, []);

  // Handle 404 error (feature disabled)
  useEffect(() => {
    if (anomalyError && anomalyError.includes('404')) {
      setFeatureEnabled(false);
    }
  }, [anomalyError]);

  const getSeverityClass = (severity) => {
    switch (severity) {
      case 'critical':
        return 'anomaly-critical';
      case 'warning':
        return 'anomaly-warning';
      default:
        return 'anomaly-info';
    }
  };

  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'critical':
        return '🔴';
      case 'warning':
        return '🟡';
      default:
        return '🔵';
    }
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return '';
    // Handle both millisecond timestamps and ISO strings
    const date = typeof timestamp === 'number' ? new Date(timestamp) : new Date(timestamp);
    if (isNaN(date.getTime())) return '';
    return date.toLocaleTimeString('en-US', { 
      hour12: false, 
      hour: '2-digit', 
      minute: '2-digit', 
      second: '2-digit',
      fractionalSecondDigits: 0
    });
  };

  const formatMetricValue = (metricName, value) => {
    if (metricName.toLowerCase().includes('temperature')) {
      return `${value.toFixed(1)}°C`;
    } else if (metricName.toLowerCase().includes('pressure')) {
      return `${value.toFixed(2)} bar`;
    }
    return value.toFixed(2);
  };

  if (!featureEnabled) {
    return (
      <div className="anomaly-panel">
        <div className="anomaly-header">
          <h3>
            <span className="anomaly-icon">⚠️</span>
            Anomaly Detection
          </h3>
        </div>
        <div className="anomaly-content">
          <div className="feature-disabled-message">
            <p>Anomaly detection feature is not enabled.</p>
            <p className="feature-hint">Enable it in <code>backend/config.yaml</code> by setting <code>features.anomaly_detection.enabled: true</code></p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="anomaly-panel">
      <div className="anomaly-header">
        <h3>
          <span className="anomaly-icon">⚠️</span>
          Anomaly Detection
        </h3>
        <div className="connection-status">
          <span className={`status-indicator ${anomalyConnected ? 'connected' : 'disconnected'}`}>
            {anomalyConnected ? 'ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>
      
      {anomalyError && (
        <div className="error-message">
          {anomalyError}
        </div>
      )}

      <div className="anomaly-content">
        {anomalies.length === 0 ? (
          <div className="no-anomalies">
            <p>No anomalies detected yet.</p>
            <p className="hint">Anomalies will appear here when detected by Flink ML_DETECT_ANOMALIES.</p>
          </div>
        ) : (
          anomalies.map((anomaly, index) => {
            const highlightId = `${anomaly.timestamp}-${anomaly.team_name}-${anomaly.metric_name}`;
            return (
              <div
                key={`${anomaly.timestamp}-${anomaly.team_name}-${anomaly.metric_name}-${index}`}
                className={`anomaly-item ${getSeverityClass(anomaly.severity)} ${
                  highlightedAnomaly === highlightId ? 'highlighted' : ''
                } ${index === 0 ? 'latest-anomaly' : ''}`}
              >
                <div className="anomaly-header-item">
                  <span className="anomaly-severity-icon">{getSeverityIcon(anomaly.severity)}</span>
                  <span className="anomaly-driver">{anomaly.driver_name || anomaly.team_name}</span>
                  <span className="anomaly-team">{anomaly.team_name}</span>
                  <span className="anomaly-time">{formatTimestamp(anomaly.timestamp)}</span>
                </div>
                <div className="anomaly-metric">
                  <span className="anomaly-metric-name">{anomaly.metric_name}:</span>
                  <span className="anomaly-metric-value">{formatMetricValue(anomaly.metric_name, anomaly.metric_value)}</span>
                </div>
                <div className="anomaly-confidence">
                  Confidence: {(anomaly.confidence_score * 100).toFixed(1)}%
                </div>
              </div>
            );
          })
        )}
        <div ref={anomalyEndRef} />
      </div>
    </div>
  );
};

export default AnomalyPanel;

