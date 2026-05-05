/**
 * Meta Marketing API module — public API.
 *
 * Единственная точка входа для остального кода wa-bridge.
 */

export { metaAdsConfig, maskToken } from './config.js';
export { metaAdsClient, MetaApiError, MetaAdsDisabledError } from './client.js';
export {
  syncAdAccount,
  syncCampaigns,
  syncAdSets,
  syncAds,
  syncCreatives,
  syncInsights,
  syncFull,
  syncDelta,
} from './sync.js';

export { metaAdsRouter } from './api.js';
