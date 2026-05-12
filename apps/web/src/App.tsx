import { Routes, Route, Navigate } from 'react-router-dom';
import { AppShell } from './components/layout/AppShell';
import { Dashboard } from './pages/Dashboard';
import { Budgets } from './pages/Budgets';
import { ConfigureLayout } from './pages/Configure/ConfigureLayout';
import { DataSources } from './pages/Configure/DataSources';
import { Catalog } from './pages/Configure/Catalog';
import { Transformations } from './pages/Configure/Transformations';
import { GovernedTags } from './pages/Configure/GovernedTags';
import { Credentials } from './pages/ExternalData/Credentials';
import { ExploreStub } from './pages/Explore/ExploreStub';
import { Genie } from './pages/Explore/Genie';

export function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/overview" replace />} />
        <Route path="/overview" element={<Dashboard />} />
        <Route path="/budgets" element={<Budgets />} />
        <Route path="/genie" element={<Genie />} />
        <Route
          path="/query"
          element={<ExploreStub titleKey="explore.query.title" descKey="explore.query.desc" />}
        />

        <Route element={<ConfigureLayout />}>
          <Route path="/data-sources" element={<DataSources />} />
          <Route path="/tags" element={<GovernedTags />} />
          <Route path="/transformations" element={<Transformations />} />
          <Route path="/credentials" element={<Credentials />} />
          <Route path="/admin" element={<Catalog />} />
        </Route>

        <Route path="/configure" element={<Navigate to="/data-sources" replace />} />
        <Route path="/configure/data-sources" element={<Navigate to="/data-sources" replace />} />
        <Route path="/configure/credentials" element={<Navigate to="/credentials" replace />} />
        <Route
          path="/configure/transformations"
          element={<Navigate to="/transformations" replace />}
        />
        <Route path="/configure/catalog" element={<Navigate to="/admin" replace />} />
        <Route path="/catalog" element={<Navigate to="/admin" replace />} />

        <Route path="/storage-credentials" element={<Navigate to="/credentials" replace />} />
        <Route path="/bcm-credentials" element={<Navigate to="/credentials" replace />} />

        <Route path="/settings" element={<Navigate to="/admin" replace />} />
        <Route path="/setup" element={<Navigate to="/data-sources" replace />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </AppShell>
  );
}
