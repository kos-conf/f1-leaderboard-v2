import React, { useState, useEffect } from 'react';
import Leaderboard from './components/Leaderboard';
import CommentaryPanel from './components/CommentaryPanel';
import AnomalyPanel from './components/AnomalyPanel';
import DriverSelection from './components/DriverSelection';
import './App.css';

function App() {
  const [selectedDriver, setSelectedDriver] = useState(null);
  const [raceId, setRaceId] = useState(null);
  const [raceStatus, setRaceStatus] = useState(null);
  const [showMainApp, setShowMainApp] = useState(false);
  const [isStartingRace, setIsStartingRace] = useState(false);
  const [currentTime, setCurrentTime] = useState(new Date());
  const [isRaceFinished, setIsRaceFinished] = useState(false);
  const [showRaceStarting, setShowRaceStarting] = useState(false);
  const [showRaceFinished, setShowRaceFinished] = useState(false);
  const [finalPositions, setFinalPositions] = useState(null);
  const [commentaryReceived, setCommentaryReceived] = useState(false);
  const [showAnomaliesPage, setShowAnomaliesPage] = useState(false);
  const [showAnomaliesModal, setShowAnomaliesModal] = useState(false);
  const [anomalies, setAnomalies] = useState([]);
  const [loadingAnomalies, setLoadingAnomalies] = useState(false);

  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);

    return () => clearInterval(timer);
  }, []);

  // Poll race status when race is active
  useEffect(() => {
    if (!raceId || isRaceFinished) return;

    const pollRaceStatus = async () => {
      try {
        const response = await fetch(`http://localhost:8001/api/race/${raceId}/status`);
        if (response.ok) {
          const status = await response.json();
          setRaceStatus(status);
          
          // If race is finished, stop polling immediately
          if (status.status === 'finished') {
            console.log('Race finished!');
            setIsRaceFinished(true);
            
            // Fetch final positions but don't show race finished animation yet
            fetchFinalPositions();
          }
        }
      } catch (error) {
        console.error('Error polling race status:', error);
      }
    };

    const interval = setInterval(pollRaceStatus, 1000);
    return () => clearInterval(interval);
  }, [raceId, isRaceFinished]);

  const fetchFinalPositions = async () => {
    try {
      console.log('Fetching final positions for race:', raceId);
      
      // Add a small delay to ensure we get the latest position data
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      const response = await fetch(`http://localhost:8001/api/race/${raceId}/positions`);
      if (response.ok) {
        const data = await response.json();
        console.log('Final positions data received:', data);
        console.log('Positions details:', data.positions?.map(p => `${p.driver_name}: P${p.position}`));
        setFinalPositions(data.positions || data);
        
        // Don't show race finished animation yet - wait for commentary
        console.log('Final positions fetched, waiting for commentary...');
      } else {
        console.error('Failed to fetch final positions:', response.status, response.statusText);
      }
    } catch (error) {
      console.error('Error fetching final positions:', error);
    }
  };

  const handleDriverSelect = async (driver) => {
    setIsStartingRace(true);
    setShowRaceStarting(true);
    
    try {
      // Start a new race
      const response = await fetch('http://localhost:8001/api/race/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ driver_name: driver.name }),
      });

      if (response.ok) {
        const raceData = await response.json();
        setSelectedDriver(driver);
        setRaceId(raceData.race_id);
        
        // Show race starting animation for 3 seconds, then show main app
        setTimeout(() => {
          setShowRaceStarting(false);
          setShowMainApp(true);
        }, 3000);
      } else {
        console.error('Failed to start race');
        setShowRaceStarting(false);
      }
    } catch (error) {
      console.error('Error starting race:', error);
      setShowRaceStarting(false);
    } finally {
      setIsStartingRace(false);
    }
  };

  const handleCommentaryReceived = () => {
    if (isRaceFinished && !commentaryReceived) {
      setCommentaryReceived(true);
      console.log('Commentary received, showing race finished animation...');
      
      // Show race finished animation after commentary is received
      setTimeout(() => {
        setShowRaceFinished(true);
      }, 1000);
    }
  };

  // Fallback timeout in case commentary doesn't arrive
  useEffect(() => {
    if (isRaceFinished && !commentaryReceived) {
      const timeout = setTimeout(() => {
        console.log('Commentary timeout, showing race finished animation anyway...');
        setCommentaryReceived(true);
        setShowRaceFinished(true);
      }, 10000); // 10 second timeout

      return () => clearTimeout(timeout);
    }
  }, [isRaceFinished, commentaryReceived]);

  const handleBackToSelection = () => {
    setShowMainApp(false);
    setSelectedDriver(null);
    setRaceId(null);
    setRaceStatus(null);
    setIsRaceFinished(false);
    setShowRaceStarting(false);
    setShowRaceFinished(false);
    setFinalPositions(null);
    setCommentaryReceived(false);
    setShowAnomaliesPage(false);
  };

  const handleViewAnomalies = () => {
    setShowAnomaliesPage(true);
    setShowMainApp(true);
  };

  const handleBackFromAnomalies = () => {
    setShowAnomaliesPage(false);
    setShowMainApp(false);
  };

  const fetchAnomalies = async () => {
    setLoadingAnomalies(true);
    
    try {
      const url = raceId 
        ? `http://localhost:8001/api/anomalies/latest?race_id=${raceId}`
        : 'http://localhost:8001/api/anomalies/latest';
      
      const response = await fetch(url);
      if (response.ok) {
        const data = await response.json();
        setAnomalies(data.anomalies || []);
      } else if (response.status === 404) {
        // Feature not enabled
        setAnomalies([]);
      } else {
        console.error('Failed to fetch anomalies:', response.status);
        setAnomalies([]);
      }
    } catch (error) {
      console.error('Error fetching anomalies:', error);
      setAnomalies([]);
    } finally {
      setLoadingAnomalies(false);
    }
  };

  const handleAnomaliesButtonClick = async () => {
    setShowAnomaliesModal(true);
    await fetchAnomalies();
  };

  const handleCloseAnomaliesModal = () => {
    setShowAnomaliesModal(false);
  };

  const formatTime = (date) => {
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const getRaceProgress = () => {
    if (!raceStatus) return 0;
    return Math.round(raceStatus.progress * 100);
  };

  const getRemainingTime = () => {
    if (!raceStatus) return 60;
    return Math.max(0, Math.round(raceStatus.remaining_time));
  };

  const getChosenDriverPosition = () => {
    if (!finalPositions || !selectedDriver) {
      console.log('No finalPositions or selectedDriver:', { finalPositions, selectedDriver });
      return null;
    }
    const chosenDriver = finalPositions.find(driver => driver.driver_name === selectedDriver.name);
    console.log('Chosen driver position:', chosenDriver);
    return chosenDriver;
  };

  const getRaceWinner = () => {
    if (!finalPositions) {
      console.log('No finalPositions available');
      return null;
    }
    console.log('Final positions:', finalPositions);
    const winner = finalPositions.find(driver => driver.position === 1);
    console.log('Race winner:', winner);
    return winner;
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <div className="digital-clock">
            {formatTime(currentTime)}
          </div>
          <div className="timer-display">
            {raceStatus ? `${getRemainingTime()}s` : '60s'}
          </div>
        </div>
        <h1>Live F1 Leaderboard</h1>
        <p>Real-time driver positions and commentary</p>
        {showMainApp && selectedDriver && (
          <button className="back-button" onClick={handleBackToSelection}>
            ← Change Driver
          </button>
        )}
      </header>
      
      {showRaceStarting && (
        <div className="race-starting-overlay">
          <div className="race-starting-animation">
            <div className="race-starting-content">
              <div className="race-starting-icon">🏁</div>
              <h2 className="race-starting-title">Race Starting!</h2>
              <p className="race-starting-subtitle">Get ready for {selectedDriver?.name}</p>
              <div className="race-starting-countdown">
                <div className="countdown-number">3</div>
                <div className="countdown-number">2</div>
                <div className="countdown-number">1</div>
                <div className="countdown-number go">GO!</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {showRaceFinished && (
        <div className="race-finished-overlay">
          <div className="race-finished-animation">
            <div className="race-finished-content">
              <div className="race-finished-icon">🏆</div>
              <h2 className="race-finished-title">Race Finished!</h2>
              
              <div className="race-results">
                <div className="race-winner">
                  <h3>Winner</h3>
                  <div className="winner-info">
                    <span className="winner-position">1st</span>
                    <span className="winner-name">{getRaceWinner()?.driver_name || 'Loading...'}</span>
                    <span className="winner-team">🏆 Winner</span>
                  </div>
                </div>
                
                <div className="chosen-driver-result">
                  <h3>Your Driver</h3>
                  <div className="driver-result-info">
                    <span className="driver-position">#{getChosenDriverPosition()?.position || '?'}</span>
                    <span className="driver-name">{selectedDriver?.name || 'Unknown Driver'}</span>
                    <span className="driver-team">{selectedDriver?.team || 'Unknown Team'}</span>
                  </div>
                </div>
              </div>
              
              <button 
                className="continue-button" 
                onClick={() => setShowRaceFinished(false)}
              >
                Continue to Leaderboard
              </button>
            </div>
          </div>
        </div>
      )}
      
      <main className={`app-main ${!showMainApp ? 'selection-mode' : ''} ${showAnomaliesPage ? 'anomalies-mode' : ''}`}>
        {!showMainApp ? (
          <div className="selection-container">
            <DriverSelection 
              onDriverSelect={handleDriverSelect}
              selectedDriver={selectedDriver}
              isStartingRace={isStartingRace}
              isRaceFinished={isRaceFinished}
              raceId={raceId}
              onViewAnomalies={handleViewAnomalies}
            />
          </div>
        ) : showAnomaliesPage ? (
          <div className="anomalies-page">
            <div className="anomalies-page-header">
              <button className="back-button" onClick={handleBackFromAnomalies}>
                ← Back to Home
              </button>
              <h2>Anomaly Detection</h2>
            </div>
            <div className="anomalies-page-content">
              <AnomalyPanel raceId={raceId} />
            </div>
          </div>
        ) : (
          <>
            {raceStatus && (
              <div className="race-progress">
                <div className="race-info">
                  {isRaceFinished ? (
                    <>
                      <span className="race-finished">🏁 Race</span>
                      <span className="race-finished">Finished!</span>
                      <span className="race-progress-text">Final Progress: {getRaceProgress()}%</span>
                    </>
                  ) : (
                    <>
                      <span>Race Progress: {getRaceProgress()}%</span>
                      <span>Time Remaining: {getRemainingTime()}s</span>
                    </>
                  )}
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ width: `${getRaceProgress()}%` }}
                  ></div>
                </div>
              </div>
            )}
            
            <div className="leaderboard-section">
              <Leaderboard 
                selectedDriver={selectedDriver} 
                raceId={raceId}
                raceStatus={raceStatus}
              />
            </div>
            
            <div className="commentary-section">
              <CommentaryPanel 
                selectedDriver={selectedDriver} 
                onCommentaryReceived={handleCommentaryReceived}
                isRaceFinished={isRaceFinished}
              />
            </div>
          </>
        )}
      </main>

      {/* Always visible anomalies button */}
      <div className="anomalies-button-section">
        <button 
          className="view-anomalies-button"
          onClick={handleAnomaliesButtonClick}
        >
          <span className="anomaly-icon">⚠️</span>
          View Anomalies
        </button>
      </div>

      {/* Anomalies Modal */}
      {showAnomaliesModal && (
        <div className="anomalies-modal-overlay" onClick={handleCloseAnomaliesModal}>
          <div className="anomalies-modal" onClick={(e) => e.stopPropagation()}>
            <div className="anomalies-modal-header">
              <h2>Anomaly Detection</h2>
              <div className="modal-header-actions">
                <button 
                  className="modal-refresh-button" 
                  onClick={fetchAnomalies}
                  disabled={loadingAnomalies}
                  title="Refresh anomalies"
                >
                  🔄
                </button>
                <button className="modal-close-button" onClick={handleCloseAnomaliesModal}>
                  ×
                </button>
              </div>
            </div>
            <div className="anomalies-modal-content">
              {loadingAnomalies ? (
                <div className="loading">
                  <div className="loading-spinner"></div>
                  <p>Loading anomalies...</p>
                </div>
              ) : anomalies.length === 0 ? (
                <div className="no-anomalies">
                  <p>No anomalies detected yet.</p>
                  <p className="hint">Anomalies will appear here when detected by Flink ML_DETECT_ANOMALIES.</p>
                </div>
              ) : (
                <div className="anomalies-list">
                  {anomalies.map((anomaly, index) => {
                    const getSeverityClass = (severity) => {
                      switch (severity) {
                        case 'critical': return 'anomaly-critical';
                        case 'warning': return 'anomaly-warning';
                        default: return 'anomaly-info';
                      }
                    };
                    
                    const getSeverityIcon = (severity) => {
                      switch (severity) {
                        case 'critical': return '🔴';
                        case 'warning': return '🟡';
                        default: return '🔵';
                      }
                    };
                    
                    const formatTimestamp = (timestamp) => {
                      if (!timestamp) return '';
                      const date = typeof timestamp === 'number' ? new Date(timestamp) : new Date(timestamp);
                      if (isNaN(date.getTime())) return '';
                      return date.toLocaleTimeString('en-US', { 
                        hour12: false, 
                        hour: '2-digit', 
                        minute: '2-digit', 
                        second: '2-digit'
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
                    
                    return (
                      <div
                        key={`${anomaly.timestamp}-${anomaly.team_name}-${anomaly.metric_name}-${index}`}
                        className={`anomaly-item ${getSeverityClass(anomaly.severity)}`}
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
                          Confidence: {((anomaly.confidence_score || 0) * 100).toFixed(1)}%
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;