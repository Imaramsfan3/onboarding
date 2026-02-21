import React, { useState } from 'react';
import { submitBusinessDetails, searchCompany } from '../services/api';

function BusinessDetails({ applicationId, data, onSave, onNext, onBack }) {
  const [form, setForm] = useState({
    company_name: data.company_name || '',
    company_number: data.company_number || '',
    registered_address: data.registered_address || '',
    incorporation_date: data.incorporation_date || '',
    company_type: data.company_type || '',
  });
  const [searchQuery, setSearchQuery] = useState('');
  const [companies, setCompanies] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handleCompanySearch = async () => {
    if (!searchQuery.trim()) return;

    setSearchError('');
    setSearchLoading(true);
    setCompanies([]);

    try {
      const result = await searchCompany(searchQuery);
      const companyList = result.companies || result.items || result || [];
      if (Array.isArray(companyList) && companyList.length > 0) {
        setCompanies(companyList);
      } else {
        setSearchError('No companies found matching your search.');
      }
    } catch (err) {
      const message =
        err.response?.data?.detail ||
        err.response?.data?.message ||
        'Failed to search companies. Please try again.';
      setSearchError(message);
    } finally {
      setSearchLoading(false);
    }
  };

  const handleSelectCompany = (company) => {
    setForm({
      company_name: company.company_name || company.title || '',
      company_number: company.company_number || '',
      registered_address:
        company.registered_address ||
        company.address_snippet ||
        company.registered_office_address ||
        '',
      incorporation_date:
        company.incorporation_date || company.date_of_creation || '',
      company_type: company.company_type || '',
    });
    setCompanies([]);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSubmitting(true);

    try {
      await submitBusinessDetails({
        application_id: applicationId,
        company_name: form.company_name,
        company_number: form.company_number,
        registered_address: form.registered_address,
        incorporation_date: form.incorporation_date,
        company_type: form.company_type,
      });
      onSave(form);
      onNext();
    } catch (err) {
      const message =
        err.response?.data?.detail ||
        err.response?.data?.message ||
        'Failed to submit business details. Please try again.';
      setError(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="card">
      <h2 className="card-title">Business Details</h2>
      <p className="card-subtitle">
        Search for your company or enter the details manually.
      </p>

      {error && <div className="error-message">{error}</div>}

      {/* Company Search */}
      <div className="search-row" style={{ marginBottom: '1.25rem' }}>
        <div className="form-group">
          <label className="form-label" htmlFor="company-search">
            Company Search
          </label>
          <input
            id="company-search"
            type="text"
            className="form-input"
            placeholder="Search by company name or number"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                handleCompanySearch();
              }
            }}
          />
        </div>
        <button
          type="button"
          className="btn btn-search"
          onClick={handleCompanySearch}
          disabled={searchLoading || !searchQuery.trim()}
        >
          {searchLoading ? 'Searching...' : 'Search'}
        </button>
      </div>

      {searchError && <div className="error-message">{searchError}</div>}

      {companies.length > 0 && (
        <div className="dropdown-container">
          <span className="dropdown-label">
            Select a company ({companies.length} found):
          </span>
          <div className="results-list">
            {companies.map((company, idx) => (
              <div
                key={idx}
                className="results-item"
                onClick={() => handleSelectCompany(company)}
              >
                <strong>
                  {company.company_name || company.title}
                </strong>
                {' '}
                <span style={{ color: '#6b7280', fontSize: '0.8125rem' }}>
                  ({company.company_number})
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label className="form-label" htmlFor="company_name">
            Company Name <span className="required">*</span>
          </label>
          <input
            id="company_name"
            name="company_name"
            type="text"
            className="form-input"
            placeholder="Enter company name"
            value={form.company_name}
            onChange={handleChange}
            required
          />
        </div>

        <div className="form-row">
          <div className="form-group">
            <label className="form-label" htmlFor="company_number">
              Company Number <span className="required">*</span>
            </label>
            <input
              id="company_number"
              name="company_number"
              type="text"
              className="form-input"
              placeholder="e.g. 12345678"
              value={form.company_number}
              onChange={handleChange}
              required
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="company_type">
              Company Type <span className="required">*</span>
            </label>
            <input
              id="company_type"
              name="company_type"
              type="text"
              className="form-input"
              placeholder="e.g. ltd, plc"
              value={form.company_type}
              onChange={handleChange}
              required
            />
          </div>
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="registered_address">
            Registered Address <span className="required">*</span>
          </label>
          <input
            id="registered_address"
            name="registered_address"
            type="text"
            className="form-input"
            placeholder="Full registered address"
            value={form.registered_address}
            onChange={handleChange}
            required
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="incorporation_date">
            Incorporation Date <span className="required">*</span>
          </label>
          <input
            id="incorporation_date"
            name="incorporation_date"
            type="date"
            className="form-input"
            value={form.incorporation_date}
            onChange={handleChange}
            required
          />
        </div>

        <div className="btn-group">
          <button
            type="button"
            className="btn btn-secondary"
            onClick={onBack}
            disabled={submitting}
          >
            Back
          </button>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={submitting}
          >
            {submitting ? 'Submitting...' : 'Submit Application'}
          </button>
        </div>
      </form>
    </div>
  );
}

export default BusinessDetails;
