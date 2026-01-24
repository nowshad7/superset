/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { t } from '@superset-ui/core';
import { SupersetTheme } from '@apache-superset/core/ui';
import { Switch } from '@superset-ui/core/components/Switch';
import { Select } from '@superset-ui/core/components';
import {
  InfoTooltip,
  LabeledErrorBoundInput as ValidatedInput,
} from '@superset-ui/core/components';
import { FieldPropTypes } from '../../types';
import { toggleStyle, infoTooltip } from '../styles';
import { safeJsonObjectParse } from 'src/components/JsonModal/utils';

export const hostField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    isValidating={isValidating}
    id="host"
    name="host"
    value={db?.parameters?.host}
    required={required}
    hasTooltip
    tooltipText={t(
      'This can be either an IP address (e.g. 127.0.0.1) or a domain name (e.g. mydatabase.com).',
    )}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.host}
    placeholder={t('e.g. 127.0.0.1')}
    className="form-group-w-50"
    label={t('Host')}
    onChange={changeMethods.onParametersChange}
  />
);

export const portField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <>
    <ValidatedInput
      id="port"
      name="port"
      type="number"
      isValidating={isValidating}
      required={required}
      value={db?.parameters?.port as number}
      validationMethods={{ onBlur: getValidation }}
      errorMessage={validationErrors?.port}
      placeholder={t('e.g. 5432')}
      className="form-group-w-50"
      label={t('Port')}
      onChange={changeMethods.onParametersChange}
    />
  </>
);
export const httpPath = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => {
  // Use safeJsonObjectParse to avoid throwing when `db.extra` is not a JSON string
  const parsedExtra = safeJsonObjectParse(db?.extra);
  const extraJson = (parsedExtra as Record<string, any>) ?? {};
  return (
    <ValidatedInput
      isValidating={isValidating}
      id="http_path"
      name="http_path"
      required={required}
      value={extraJson.engine_params?.connect_args?.http_path}
      validationMethods={{ onBlur: getValidation }}
      errorMessage={validationErrors?.http_path}
      placeholder={t('e.g. sql/protocolv1/o/12345')}
      label="HTTP Path"
      onChange={changeMethods.onExtraInputChange}
      helpText={t('Copy the name of the HTTP Path of your cluster.')}
    />
  );
};
export const databaseField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  placeholder,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    isValidating={isValidating}
    id="database"
    name="database"
    required={required}
    value={db?.parameters?.database}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.database}
    placeholder={placeholder ?? t('e.g. world_population')}
    label={t('Database name')}
    onChange={changeMethods.onParametersChange}
    helpText={t('Copy the name of the database you are trying to connect to.')}
  />
);
export const defaultCatalogField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    isValidating={isValidating}
    id="default_catalog"
    name="default_catalog"
    required={required}
    value={db?.parameters?.default_catalog}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.default_catalog}
    placeholder={t('e.g. hive_metastore')}
    label={t('Default Catalog')}
    onChange={changeMethods.onParametersChange}
    helpText={t('The default catalog that should be used for the connection.')}
  />
);
export const defaultSchemaField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="default_schema"
    name="default_schema"
    required={required}
    isValidating={isValidating}
    value={db?.parameters?.default_schema}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.default_schema}
    placeholder={t('e.g. default')}
    label={t('Default Schema')}
    onChange={changeMethods.onParametersChange}
    helpText={t('The default schema that should be used for the connection.')}
  />
);
export const httpPathField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="http_path_field"
    name="http_path_field"
    required={required}
    isValidating={isValidating}
    value={db?.parameters?.http_path_field}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.http_path}
    placeholder={t('e.g. sql/protocolv1/o/12345')}
    label="HTTP Path"
    onChange={changeMethods.onParametersChange}
    helpText={t('Copy the name of the HTTP Path of your cluster.')}
  />
);
export const usernameField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="username"
    name="username"
    required={required}
    isValidating={isValidating}
    value={db?.parameters?.username}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.username}
    placeholder={t('e.g. Analytics')}
    label={t('Username')}
    onChange={changeMethods.onParametersChange}
  />
);
export const passwordField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isEditMode,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="password"
    name="password"
    required={required}
    isValidating={isValidating}
    visibilityToggle={!isEditMode}
    value={db?.parameters?.password}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.password}
    placeholder={t('e.g. ********')}
    label={t('Password')}
    onChange={changeMethods.onParametersChange}
  />
);
export const accessTokenField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isEditMode,
  default_value: defaultValue,
  description,
}: FieldPropTypes) => (
  <ValidatedInput
    id="oauth2_access_token"
    name="oauth2_access_token"
    required={required}
    visibilityToggle={!isEditMode}
    // Prefer the namespaced oauth2 key but remain compatible with older 'access_token'
    value={db?.parameters?.oauth2_access_token || db?.parameters?.access_token}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.oauth2_access_token || validationErrors?.access_token}
    placeholder={t('Paste your OAuth2 access token here')}
    get_url={
      typeof defaultValue === 'string' && defaultValue.includes('https://')
        ? defaultValue
        : null
    }
    description={description}
    label={t('Access token')}
    onChange={changeMethods.onParametersChange}
  />
);
export const displayField = ({
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <>
    <ValidatedInput
      id="database_name"
      name="database_name"
      required
      isValidating={isValidating}
      value={db?.database_name}
      validationMethods={{ onBlur: getValidation }}
      errorMessage={validationErrors?.database_name}
      placeholder=""
      label={t('Display Name')}
      onChange={changeMethods.onChange}
      helpText={t(
        'Pick a nickname for how the database will display in Superset.',
      )}
    />
  </>
);

export const queryField = ({
  required,
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="query_input"
    name="query_input"
    required={required}
    isValidating={isValidating}
    value={db?.query_input || ''}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.query}
    placeholder={t('e.g. param1=value1&param2=value2')}
    label={t('Additional Parameters')}
    onChange={changeMethods.onQueryChange}
    helpText={t('Add additional custom parameters')}
  />
);

export const forceSSLField = ({
  isEditMode,
  changeMethods,
  db,
  sslForced,
}: FieldPropTypes) => (
  <div css={(theme: SupersetTheme) => infoTooltip(theme)}>
    <Switch
      disabled={sslForced && !isEditMode}
      checked={db?.parameters?.encryption || sslForced}
      onChange={changed => {
        changeMethods.onParametersChange({
          target: {
            type: 'toggle',
            name: 'encryption',
            checked: true,
            value: changed,
          },
        });
      }}
    />
    <span css={toggleStyle}>SSL</span>
    <InfoTooltip
      tooltip={t('SSL Mode "require" will be used.')}
      placement="right"
    />
  </div>
);

export const projectIdfield = ({
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <>
    <ValidatedInput
      id="project_id"
      name="project_id"
      required
      isValidating={isValidating}
      value={db?.parameters?.project_id}
      validationMethods={{ onBlur: getValidation }}
      errorMessage={validationErrors?.project_id}
      placeholder="your-project-1234-a1"
      label={t('Project Id')}
      onChange={changeMethods.onParametersChange}
      helpText={t('Enter the unique project id for your database.')}
    />
  </>
);

export const endpointField = ({
  changeMethods,
  getValidation,
  validationErrors,
  db,
  isValidating,
}: FieldPropTypes) => (
  <ValidatedInput
    id="endpoint"
    name="endpoint"
    required
    isValidating={isValidating}
    value={db?.parameters?.endpoint || ''}
    validationMethods={{ onBlur: getValidation }}
    errorMessage={validationErrors?.endpoint}
    placeholder={t('https://api.example.com/v1/resource')}
    label={t('API Endpoint')}
    onChange={changeMethods.onParametersChange}
    helpText={t('The base URL for the JSON API to query.')}
  />
);

export const authConfigTypeField = ({ changeMethods, db }: FieldPropTypes) => {
  const value = db?.parameters?.auth_config_type || 'no_auth';
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ marginBottom: 8 }}>{t('Authentication type')}</div>
      <Select
        value={value}
        onChange={(v: string) =>
          changeMethods.onParametersChange({
            target: { name: 'auth_config_type', value: v },
          })
        }
        options={[
          { value: 'no_auth', label: t('No auth') },
          { value: 'superset_auth', label: t('Superset Auth Deligation') },
          { value: 'basic_auth', label: t('Basic Auth') },
          { value: 'api_key', label: t('API Key') },
          { value: 'oauth2', label: t('OAuth2') },
        ]}
      />
    </div>
  );
};

export const apiKeyField = ({
  changeMethods,
  validationErrors,
  db,
}: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'api_key') return null;
  return (
    <ValidatedInput
      id="api_key"
      name="api_key"
      required
      value={db?.parameters?.api_key || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.api_key}
      placeholder={t('Paste your API key here')}
      label={t('API Key')}
      onChange={changeMethods.onParametersChange}
    />
  );
};

export const basicAuthUsernameField = ({
  changeMethods,
  db,
}: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'basic_auth') return null;
  return (
    <ValidatedInput
      id="basic_auth_username"
      name="basic_auth_username"
      value={db?.parameters?.basic_auth_username || ''}
      label={t('Basic Auth Username')}
      onChange={changeMethods.onParametersChange}
      validationMethods={{ onBlur: () => {} }}
    />
  );
};

export const basicAuthPasswordField = ({
  changeMethods,
  db,
  isEditMode,
  validationErrors,
  isValidating,
}: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'basic_auth') return null;
  return (
    <ValidatedInput
      id="basic_auth_password"
      name="basic_auth_password"
      visibilityToggle={!isEditMode}
      isValidating={isValidating}
      value={db?.parameters?.basic_auth_password || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.basic_auth_password}
      placeholder={t('e.g. ********')}
      label={t('Basic Auth Password')}
      onChange={changeMethods.onParametersChange}
    />
  );
};

// OAuth2 fields: show when auth_config_type === 'oauth2'
export const oauth2ClientIdField = ({ changeMethods, db, validationErrors }: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'oauth2') return null;
  return (
    <ValidatedInput
      id="oauth2_client_id"
      name="oauth2_client_id"
      value={db?.parameters?.oauth2_client_id || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.oauth2_client_id}
      placeholder={t('OAuth2 Client ID')}
      label={t('Client ID')}
      onChange={changeMethods.onParametersChange}
    />
  );
};

export const oauth2ClientSecretField = ({ changeMethods, db, isEditMode, validationErrors }: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'oauth2') return null;
  return (
    <ValidatedInput
      id="oauth2_client_secret"
      name="oauth2_client_secret"
      visibilityToggle={!isEditMode}
      value={db?.parameters?.oauth2_client_secret || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.oauth2_client_secret}
      placeholder={t('OAuth2 Client Secret')}
      label={t('Client Secret')}
      onChange={changeMethods.onParametersChange}
    />
  );
};

export const oauth2TokenUrlField = ({ changeMethods, db, validationErrors }: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'oauth2') return null;
  return (
    <ValidatedInput
      id="oauth2_token_url"
      name="oauth2_token_url"
      value={db?.parameters?.oauth2_token_url || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.oauth2_token_url}
      placeholder={t('https://')}
      label={t('Token URL')}
      onChange={changeMethods.onParametersChange}
    />
  );
};

export const oauth2ScopesField = ({ changeMethods, db, validationErrors }: FieldPropTypes) => {
  if (db?.parameters?.auth_config_type !== 'oauth2') return null;
  return (
    <ValidatedInput
      id="oauth2_scopes"
      name="oauth2_scopes"
      value={db?.parameters?.oauth2_scopes || ''}
      validationMethods={{ onBlur: () => {} }}
      errorMessage={validationErrors?.oauth2_scopes}
      placeholder={t('space separated scopes')}
      label={t('Scopes')}
      onChange={changeMethods.onParametersChange}
    />
  );
};
