import path from "path";
import fs from "fs";
import rawLog from "@bksLogger";
import platformInfo from "@/common/platform_info";
import globals from "@/common/globals";
import {
  Module,
  PluginSourcePathParams,
  type ModuleOptions,
} from "@/services/plugin/Module";
import { PluginSystemDisabledError } from "@/services/plugin/errors";
import { BksConfig } from "@/common/bksConfig/BksConfigProvider";
import { Manifest } from "@/services/plugin";

const log = rawLog.scope("BundledPluginModule");

type BundledPluginOptions = {
  config: BksConfig;
  /** For testing only. */
  testIgnoreEnsureInstalled?: boolean;
};

/**
 * A plugin system module that copies bundled plugins from node_modules (dev)
 * or extraResources (prod) to the user's plugins directory on first launch.
 *
 * @example
 *
 * ```ts
 * const manager = new PluginManager();
 * manager.registerModule(BundledPluginModule);
 * await manager.initialize();
 * ```
 **/
export class BundledPluginModule extends Module {
  constructor(private options: BundledPluginOptions & ModuleOptions) {
    super(options);

    this.hook("before-initialize", this.installBundledPlugins);
    this.hook("plugin-source", this.resolvePluginSource);
  }

  static with(options: BundledPluginOptions) {
    return class extends BundledPluginModule {
      constructor(baseOptions: ModuleOptions) {
        super({ ...baseOptions, ...options });
      }
    };
  }

  private async installBundledPlugins() {
    if (this.options.testIgnoreEnsureInstalled) {
      return;
    }

    for (const plugin of globals.plugins.ensureInstalled) {
      try {
        await this.ensureInstall(plugin);
      } catch (e) {
        log.error(`Error installing plugin ${plugin}`, e);
      }
    }
  }

  private resolvePluginSource(
    params: PluginSourcePathParams
  ): PluginSourcePathParams {
    if (!this.options.config.pluginSystem.disabled) {
      return params;
    }

    if (this.options.config.pluginSystem.allow.includes(params.id)) {
      for (const pkg of globals.plugins.ensureInstalled) {
        const pluginPath = BundledPluginModule.resolve(pkg);
        const manifest = this.parseManifest(pluginPath);

        if (params.id === manifest?.id) {
          return {
            ...params,
            path: pluginPath,
            cleanupAfterInstall: false,
          };
        }
      }

      throw new PluginSystemDisabledError(
        `Cannot install "${params.id}": the plugin is in the allow list but no bundled version is available.`
      );
    }

    throw new PluginSystemDisabledError(
      `Cannot install "${params.id}": the plugin system is disabled and this plugin is not in the allow list.`
    );
  }

  /**
   * Install a plugin from a given path if it is not already installed.
   *
   * @param pkg Package name (e.g., "@beekeeperstudio/bks-ai-shell")
   */
  private async ensureInstall(pkg: string) {
    log.info(`Resolving ${pkg}`);

    const pluginPath = BundledPluginModule.resolve(pkg);

    const manifest = this.parseManifest(pluginPath);
    if (!manifest) {
      throw new Error(`Manifest not found for ${pkg}`);
    }

    const pluginId = manifest.id;

    // Have installed before?
    if (this.manager.pluginSettings[pluginId]) {
      log.info(`Plugin "${pluginId}" is previously installed, skipping.`);
      return;
    }

    const pluginsDirectory = this.manager.fileManager.options.pluginsDirectory;

    if (!fs.existsSync(pluginsDirectory)) {
      fs.mkdirSync(pluginsDirectory, { recursive: true });
    }

    const dst = path.join(pluginsDirectory, pluginId);
    if (fs.existsSync(dst)) {
      // This must be set, otherwise the plugin will be copied again
      await this.manager.setPluginAutoUpdateEnabled(pluginId, true);
      log.info(
        `Plugin "${pluginId}" installation directory already exists on disk.`
      );
      return;
    }

    log.info(`Installing plugin ${pluginId}`);
    fs.cpSync(pluginPath, dst, { recursive: true });

    // This must be set, otherwise the plugin will be copied again
    await this.manager.setPluginAutoUpdateEnabled(pluginId, true);
  }

  private parseManifest(pluginPath: string): Manifest | undefined {
    const manifestPath = path.join(pluginPath, "manifest.json");

    if (!fs.existsSync(manifestPath)) {
      return undefined;
    }

    return JSON.parse(fs.readFileSync(manifestPath, "utf-8"));
  }

  /**
   * Resolve a bundled plugin path.
   *
   * @param pkg Package name (e.g., "@beekeeperstudio/bks-ai-shell")
   * @returns The resolved path to the plugin directory
   */
  static resolve(pkg: string): string {
    if (platformInfo.env.production) {
      // Production: use extraResources location
      return path.join(platformInfo.resourcesPath, "bundled_plugins", pkg);
    }

    // Development: resolve from node_modules
    const manifestPath = require.resolve(`${pkg}/manifest.json`);
    return path.dirname(manifestPath);
  }
}
