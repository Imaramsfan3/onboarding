import React, { useState } from 'react';
import { submitPersonalDetails } from '../services/api';

function PersonalDetails({ applicationId, data, onSave, onNext }) {
  const [form, setForm] = useState({
    forename: data.forename || '',
    middle_name: data.middle_name || '',
    surname: data.surname || '',
    date_of_birth: data.date_of_birth || '',
  });
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSubmitting(true);

    try {
      await submitPersonalDetails({
        application_id: applicationId,
        forename: form.forename,
        middle_name: form.middle_name,
        surname: form.surname,
        date_of_birth: form.date_of_birth,
      });
      onSave(form);
      onNext();
    } catch (err) {
      const message =
        err.response?.data?.detail ||
        err.response?.data?.message ||
        'Failed to submit personal details. Please try again.';
      setError(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="card">
      <h2 className="card-title">Personal Details</h2>
      <p className="card-subtitle">
        Tell us about yourself to get started.
      </p>

      {error && <div className="error-message">{error}</div>}

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label className="form-label" htmlFor="forename">
            Forename <span className="required">*</span>
          </label>
          <input
            id="forename"
            name="forename"
            type="text"
            className="form-input"
            placeholder="Enter your forename"
            value={form.forename}
            onChange={handleChange}
            required
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="middle_name">
            Middle Name
          </label>
          <input
            id="middle_name"
            name="middle_name"
            type="text"
            className="form-input"
            placeholder="Enter your middle name (optional)"
            value={form.middle_name}
            onChange={handleChange}
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="surname">
            Surname <span className="required">*</span>
          </label>
          <input
            id="surname"
            name="surname"
            type="text"
            className="form-input"
            placeholder="Enter your surname"
            value={form.surname}
            onChange={handleChange}
            required
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="date_of_birth">
            Date of Birth <span className="required">*</span>
          </label>
          <input
            id="date_of_birth"
            name="date_of_birth"
            type="date"
            className="form-input"
            value={form.date_of_birth}
            onChange={handleChange}
            required
          />
        </div>

        <div className="btn-group btn-group-right">
          <button
            type="submit"
            className="btn btn-primary"
            disabled={submitting}
          >
            {submitting ? 'Submitting...' : 'Next'}
          </button>
        </div>
      </form>
    </div>
  );
}

export default PersonalDetails;
