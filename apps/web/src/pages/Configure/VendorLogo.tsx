import type { DataSourceTemplate, TemplateLogo } from './dataSourceCatalog';
import databricksLogoUrl from '../../assets/databricks-logo.svg';
import awsLogoUrl from '../../assets/aws-logo.svg';
import googleCloudLogoUrl from '../../assets/google-cloud-logo.svg';
import snowflakeLogoUrl from '../../assets/snowflake-logo.svg';

export function VendorLogo({
  source,
  logo,
  size = 56,
}: {
  source: DataSourceTemplate;
  logo?: TemplateLogo;
  size?: number;
}) {
  const imageSize = Math.round(size * 0.82);

  if (logo?.kind === 'databricks') {
    return (
      <div className="grid place-items-center" style={{ width: size, height: size }} aria-hidden>
        <img
          src={databricksLogoUrl}
          style={{ height: imageSize, width: 'auto' }}
          alt=""
          aria-hidden
        />
      </div>
    );
  }

  if (logo?.kind === 'aws') {
    return (
      <div className="grid place-items-center" style={{ width: size, height: size }} aria-hidden>
        <img
          src={awsLogoUrl}
          className="dark:invert"
          style={{ height: imageSize, width: 'auto' }}
          alt=""
          aria-hidden
        />
      </div>
    );
  }

  if (logo?.kind === 'google-cloud') {
    return (
      <div className="grid place-items-center" style={{ width: size, height: size }} aria-hidden>
        <img
          src={googleCloudLogoUrl}
          style={{ height: imageSize, width: 'auto' }}
          alt=""
          aria-hidden
        />
      </div>
    );
  }

  if (logo?.kind === 'snowflake') {
    return (
      <div className="grid place-items-center" style={{ width: size, height: size }} aria-hidden>
        <img
          src={snowflakeLogoUrl}
          style={{ height: imageSize, width: 'auto' }}
          alt=""
          aria-hidden
        />
      </div>
    );
  }

  return (
    <div
      className="vendor-logo"
      style={{
        width: size,
        height: size,
        background: source.appearance.brandColor,
        color: source.appearance.brandTextColor ?? '#ffffff',
      }}
      aria-hidden
    >
      {logo?.kind === 'abbr' ? logo.label : source.id.slice(0, 3)}
    </div>
  );
}
