import React, { useState, useEffect, useRef } from 'react';
import { getApplicationStatus } from '../services/api';

function ApplicationComplete({ applicationId }) {
  const [status, setStatus] = useState('PENDING');
  const [message, setMessage] = useState('Processing your application...');
  const [error, setError] = useState('');
  const intervalRef = useRef(null);

  useEffect(() => {
    let isMounted = true;

    const pollStatus = async () => {
      try {
        const result = await getApplicationStatus(applicationId);
        if (!isMounted) return;

        setStatus(result.status);
        setMessage(result.message || '');
        setError('');

        if (result.status === 'COMPLETE') {
          if (intervalRef.current) {
            clearInterval(intervalRef.current);
            intervalRef.current = null;
          }
        }
      } catch (err) {
        if (!isMounted) return;
        setError('Unable to check application status. Retrying...');
      }
    };

    // Poll immediately, then every 2 seconds
    pollStatus();
    intervalRef.current = setInterval(pollStatus, 2000);

    return () => {
      isMounted = false;
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [applicationId]);

  if (status === 'COMPLETE') {
    return (
      <div className="card">
        <div className="complete-container">
          <div className="complete-icon complete-icon-success">
            <svg
              width="40"
              height="40"
              viewBox="0 0 40 40"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                d="M33.3 11.3L16 28.6L6.7 19.3L9.3 16.7L16 23.4L30.7 8.7L33.3 11.3Z"
                fill="#10b981"
              />
            </svg>
          </div>
          <h2 className="complete-title">Application Complete</h2>
          <p className="complete-message">
            {message || 'Your onboarding application has been successfully processed.'}
          </p>
          <div className="complete-app-id">
            Application ID: {applicationId}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="card">
      <div className="complete-container">
        <div className="spinner" />
        <h2 className="complete-title">Processing Application</h2>
        <p className="complete-message">
          {message || 'Please wait while we process your application...'}
        </p>
        {error && (
          <div className="error-message" style={{ textAlign: 'left' }}>
            {error}
          </div>
        )}
        <p className="loading-text">
          This may take a few moments.
        </p>
        <div className="complete-app-id" style={{ marginTop: '1rem' }}>
          Application ID: {applicationId}
        </div>
      </div>
    </div>
  );
}

export default ApplicationComplete;
