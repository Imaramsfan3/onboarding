import React, { useState, useEffect, useCallback } from 'react';
import { v4 as uuidv4 } from 'uuid';
import PersonalDetails from './screens/PersonalDetails';
import AddressDetails from './screens/AddressDetails';
import BusinessDetails from './screens/BusinessDetails';
import ApplicationComplete from './screens/ApplicationComplete';

const STEPS = [
  { number: 1, label: 'Personal Details' },
  { number: 2, label: 'Address' },
  { number: 3, label: 'Business Details' },
];

function StepIndicator({ currentStep }) {
  return (
    <div className="step-indicator">
      {STEPS.map((step, index) => (
        <React.Fragment key={step.number}>
          <div
            className={`step ${
              currentStep > step.number
                ? 'step-completed'
                : currentStep === step.number
                ? 'step-active'
                : 'step-pending'
            }`}
          >
            <div className="step-circle">
              {currentStep > step.number ? (
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 16 16"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    d="M13.3 4.3L6 11.6L2.7 8.3L3.7 7.3L6 9.6L12.3 3.3L13.3 4.3Z"
                    fill="white"
                  />
                </svg>
              ) : (
                step.number
              )}
            </div>
            <span className="step-label">{step.label}</span>
          </div>
          {index < STEPS.length - 1 && (
            <div
              className={`step-connector ${
                currentStep > step.number ? 'connector-completed' : ''
              }`}
            />
          )}
        </React.Fragment>
      ))}
    </div>
  );
}

function App() {
  const [applicationId, setApplicationId] = useState(null);
  const [currentScreen, setCurrentScreen] = useState(1);
  const [formData, setFormData] = useState({
    personal: {},
    address: {},
    business: {},
  });

  useEffect(() => {
    setApplicationId(uuidv4());
  }, []);

  const handleNext = useCallback(() => {
    setCurrentScreen((prev) => prev + 1);
  }, []);

  const handleBack = useCallback(() => {
    setCurrentScreen((prev) => prev - 1);
  }, []);

  const updateFormData = useCallback((section, data) => {
    setFormData((prev) => ({ ...prev, [section]: data }));
  }, []);

  if (!applicationId) {
    return null;
  }

  const renderScreen = () => {
    switch (currentScreen) {
      case 1:
        return (
          <PersonalDetails
            applicationId={applicationId}
            data={formData.personal}
            onSave={(data) => updateFormData('personal', data)}
            onNext={handleNext}
          />
        );
      case 2:
        return (
          <AddressDetails
            applicationId={applicationId}
            data={formData.address}
            onSave={(data) => updateFormData('address', data)}
            onNext={handleNext}
            onBack={handleBack}
          />
        );
      case 3:
        return (
          <BusinessDetails
            applicationId={applicationId}
            data={formData.business}
            onSave={(data) => updateFormData('business', data)}
            onNext={handleNext}
            onBack={handleBack}
          />
        );
      case 4:
        return <ApplicationComplete applicationId={applicationId} />;
      default:
        return null;
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Onboarding Application</h1>
      </header>
      <main className="app-main">
        {currentScreen <= 3 && <StepIndicator currentStep={currentScreen} />}
        <div className="screen-container">{renderScreen()}</div>
      </main>
    </div>
  );
}

export default App;
