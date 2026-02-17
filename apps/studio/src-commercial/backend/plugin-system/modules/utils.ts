import path from "path";
import platformInfo from "@/common/platform_info";

/**
 * Resolve a bundled plugin path.
 *
 * @param pkg Package name (e.g., "@beekeeperstudio/bks-ai-shell")
 * @returns The resolved path to the plugin directory
 */
export function resolveBundledPlugin(pkg: string): string {
  if (platformInfo.env.production) {
    // Production: use extraResources location
    return path.join(platformInfo.resourcesPath, "bundled_plugins", pkg);
  }

  // Development: resolve from node_modules
  const manifestPath = require.resolve(`${pkg}/manifest.json`);
  return path.dirname(manifestPath);
}
