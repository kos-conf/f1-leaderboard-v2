import React, { useState, useEffect, useRef } from 'react';
import { useEventSource } from '../hooks/useEventSource';

const CommentaryPanel = ({ selectedDriver, onCommentaryReceived, isRaceFinished }) => {
  const [commentary, setCommentary] = useState([]);
  const [highlightedComment, setHighlightedComment] = useState(null);
  const commentaryEndRef = useRef(null);
  const { data: commentaryData, error: commentaryError, isConnected: commentaryConnected } = useEventSource('http://localhost:8001/api/commentary/stream');

  // Helper function to check if commentary message contains driver name
  const containsDriverName = (message, driverName) => {
    if (!driverName || !message) return true; // Show all messages if no driver selected
    
    const driverNameLower = driverName.toLowerCase();
    const messageLower = message.toLowerCase();
    
    // Split driver name into first and last name
    const nameParts = driverNameLower.split(' ');
    
    // More precise matching - require at least the last name to be present
    // and optionally the first name for better filtering
    const lastName = nameParts[nameParts.length - 1];
    const firstName = nameParts[0];
    
    // Must contain the last name, and if first name is provided, it should also be present
    const hasLastName = messageLower.includes(lastName);
    const hasFirstName = firstName === lastName || messageLower.includes(firstName);
    
    return hasLastName && (nameParts.length === 1 || hasFirstName);
  };

  // Auto-scroll to top when new commentary arrives (since latest is at top)
  const scrollToTop = () => {
    if (commentaryEndRef.current) {
      commentaryEndRef.current.scrollTop = 0;
    }
  };

  useEffect(() => {
    scrollToTop();
  }, [commentary]);

  // Handle new commentary data
  useEffect(() => {
    if (commentaryData?.type === 'commentary') {
      const newComment = commentaryData.data;
      
      // Show all commentary without filtering
      setCommentary(prev => {
        // Check for duplicates by ID to prevent showing same message multiple times
        const isDuplicate = prev.some(comment => comment.id === newComment.id);
        if (isDuplicate) {
          return prev; // Don't add duplicate
        }
        
        // Add new comment at the top and keep only last 50 comments
        return [newComment, ...prev].slice(0, 50);
      });
      
      // Highlight the new comment
      setHighlightedComment(newComment.id);
      
      // Remove highlight after 5 seconds
      setTimeout(() => {
        setHighlightedComment(null);
      }, 5000);
      
      // If race is finished and this is the first commentary received, notify parent
      if (isRaceFinished && onCommentaryReceived) {
        onCommentaryReceived();
      }
    }
  }, [commentaryData, isRaceFinished, onCommentaryReceived]);


  const getCommentaryTypeClass = (type) => {
    switch (type) {
      case 'highlight':
        return 'commentary-highlight';
      case 'warning':
        return 'commentary-warning';
      default:
        return 'commentary-info';
    }
  };

  return (
    <div className="commentary-panel">
      <div className="commentary-header">
        <h3>
          <svg className="w-5 h-5 mr-2 text-red-500" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10c0 3.866-3.582 7-8 7a8.841 8.841 0 01-4.083-.98L2 17l1.338-3.123C2.493 12.767 2 11.434 2 10c0-3.866 3.582-7 8-7s8 3.134 8 7zM7 9H5v2h2V9zm8 0h-2v2h2V9zM9 9h2v2H9V9z" clipRule="evenodd"></path>
          </svg>
          Commentary
        </h3>
        <div className="connection-status">
          <span className={`status-indicator ${commentaryConnected ? 'connected' : 'disconnected'}`}>
            {commentaryConnected ? 'ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>
      
      {commentaryError && (
        <div className="error-message">
          {commentaryError}
        </div>
      )}

      <div className="commentary-content">
        {commentary.map((comment, index) => (
          <div
            key={comment.id}
            className={`commentary-item ${getCommentaryTypeClass(comment.type)} ${
              highlightedComment === comment.id ? 'highlighted' : ''
            } ${index === 0 ? 'latest-comment' : ''}`}
          >
            <div className="commentary-message">
              {comment.message}
            </div>
          </div>
        ))}
        <div ref={commentaryEndRef} />
      </div>
    </div>
  );
};

export default CommentaryPanel;
