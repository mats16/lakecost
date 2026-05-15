import { Routes, Route, Navigate } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { Budgets } from './pages/Budgets';
import { ConfigureLayout } from './pages/Configure/ConfigureLayout';
import { DataSources } from './pages/Configure/DataSources';
import {
  AwsIntegrationDetail,
  DatabricksIntegrationDetail,
} from './pages/Configure/IntegrationDetails';
import { Catalog } from './pages/Configure/Catalog';
import { Transformations } from './pages/Configure/Transformations';
import { GovernedTags } from './pages/Configure/GovernedTags';
import { Pricing } from './pages/Configure/Pricing';
import { Credentials } from './pages/ExternalData/Credentials';
import { ExploreStub } from './pages/Explore/ExploreStub';
import { Genie } from './pages/Explore/Genie';
import { DatabricksOptimize } from './pages/Optimize/DatabricksOptimize';
import { OptimizeStub } from './pages/Optimize/OptimizeStub';

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/overview" replace />} />
        <Route path="/overview" element={<Dashboard />} />
        <Route
          path="/cost-explore"
          element={
            <ExploreStub titleKey="explore.costExplore.title" descKey="explore.costExplore.desc" />
          }
        />
        <Route path="/budgets" element={<Budgets />} />
        <Route path="/genie" element={<Genie />} />
        <Route path="/optimize" element={<Navigate to="/optimize/databricks" replace />} />
        <Route path="/optimize/databricks" element={<DatabricksOptimize />} />
        <Route
          path="/optimize/aws"
          element={<OptimizeStub titleKey="optimize.aws.title" descKey="optimize.aws.desc" />}
        />

        <Route element={<ConfigureLayout />}>
          <Route path="/integrations" element={<DataSources />} />
          <Route path="/integrations/databricks" element={<DatabricksIntegrationDetail />} />
          <Route path="/integrations/aws" element={<AwsIntegrationDetail />} />
          <Route path="/tags" element={<GovernedTags />} />
          <Route path="/transformations" element={<Transformations />} />
          <Route path="/pricing" element={<Navigate to="/pricing/aws" replace />} />
          <Route path="/pricing/aws" element={<Pricing />} />
          <Route path="/credentials" element={<Credentials />} />
          <Route path="/admin" element={<Catalog />} />
        </Route>

        <Route path="/configure" element={<Navigate to="/integrations" replace />} />
        <Route path="/configure/data-sources" element={<Navigate to="/integrations" replace />} />
        <Route path="/data-sources" element={<Navigate to="/integrations" replace />} />
        <Route path="/configure/credentials" element={<Navigate to="/credentials" replace />} />
        <Route
          path="/configure/transformations"
          element={<Navigate to="/transformations" replace />}
        />
        <Route path="/configure/pricing" element={<Navigate to="/pricing/aws" replace />} />
        <Route path="/configure/catalog" element={<Navigate to="/admin" replace />} />
        <Route path="/catalog" element={<Navigate to="/admin" replace />} />

        <Route path="/storage-credentials" element={<Navigate to="/credentials" replace />} />
        <Route path="/bcm-credentials" element={<Navigate to="/credentials" replace />} />

        <Route path="/settings" element={<Navigate to="/admin" replace />} />
        <Route path="/setup" element={<Navigate to="/integrations" replace />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </AppShell>
  );
}
