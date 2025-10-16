import React, { useState, useEffect } from 'react';

const DriverRow = ({ driver, position, isHighlighted, isSelectedDriver, teamColor, previousPosition }) => {
  const [positionChange, setPositionChange] = useState(0);
  const [isAnimating, setIsAnimating] = useState(false);

  useEffect(() => {
    if (previousPosition && previousPosition !== position) {
      const change = previousPosition - position;
      setPositionChange(change);
      setIsAnimating(true);
      
      // Reset animation after 2 seconds
      setTimeout(() => {
        setIsAnimating(false);
        setPositionChange(0);
      }, 2000);
    }
  }, [position, previousPosition]);

  const getPositionChangeIcon = () => {
    if (positionChange > 0) return '⬆️';
    if (positionChange < 0) return '⬇️';
    return '';
  };

  const getPositionChangeClass = () => {
    if (positionChange > 0) return 'position-gain';
    if (positionChange < 0) return 'position-loss';
    return '';
  };

  const getPositionBadgeClass = () => {
    if (position <= 3) return 'podium';
    if (position <= 6) return 'points';
    return 'regular';
  };

  return (
    <tr 
      className={`driver-row ${isHighlighted ? 'highlighted' : ''} ${isSelectedDriver ? 'selected-driver' : ''} ${isAnimating ? 'animating' : ''}`}
    >
      <td className="position-cell">
        {position}
      </td>
      <td className="driver-cell">
        <div className="driver-info">
          <img 
            src={`https://placehold.co/40x40/${teamColor?.replace('#', '')}/FFFFFF?text=${driver?.name?.charAt(0) || 'D'}`}
            alt={driver?.name || 'Driver'}
            className="driver-logo"
            style={{ borderColor: teamColor }}
          />
          <div className="driver-details">
            <div className="driver-name">{driver?.name || 'Unknown Driver'}</div>
            <div className="team-name">{driver?.team || 'Unknown Team'}</div>
          </div>
        </div>
      </td>
      <td className="hidden sm:table-cell team-cell">
        {driver?.team || 'Unknown Team'}
      </td>
    </tr>
  );
};

export default DriverRow;
