import _ from "lodash";
import { PluginSnapshot } from "@/services/plugin";
import { Module, ModuleOptions } from "@/services/plugin/Module";
import { PluginSystemDisabledError } from "@/services/plugin/errors";
import { BksConfig } from "@/common/bksConfig/BksConfigProvider";

type ConfigurationOptions = {
  config: BksConfig;
};

/**
 * Handles plugin configuration via `config.ini`.
 *
 * @example
 *
 * ```ts
 * // Register the module
 * const pluginManager = new PluginManager({ ... });
 * pluginManager.registerModule(ConfigurationModule.with({ config: bksConfig }));
 * // Initialize the plugin manager
 * pluginManager.initialize();
 *
 * ```
 *
 * The ini file contains the following:
 *
 * ```ini
 * [plugins.general]
 * communityDisabled = true
 * ```
 */
export class ConfigurationModule extends Module {
  private config: BksConfig;

  constructor(options: ConfigurationOptions & ModuleOptions) {
    super(options);

    this.config = options.config;

    if (this.config.pluginSystem.disabled) {
      this.manager.registry.communityDisabled = true;
      this.manager.registry.officialDisabled = true;
    }

    if (this.config.pluginSystem.communityDisabled) {
      this.manager.registry.communityDisabled = true;
    }

    this.hook("before-install-plugin", this.beforeInstallGuard);
    this.hook("plugin-snapshots", this.applyConfig);
  }

  static with(options: ConfigurationOptions) {
    return class extends ConfigurationModule {
      constructor(baseOptions: ModuleOptions) {
        super({ ...baseOptions, ...options });
      }
    };
  }

  private beforeInstallGuard() {
    if (this.config.pluginSystem.disabled) {
      throw new PluginSystemDisabledError();
    }
  }

  private applyConfig(snapshots: PluginSnapshot[]): PluginSnapshot[] {
    return snapshots.map((snapshot) => {
      // Do not override disable state
      if (snapshot.disableState.disabled) {
        return snapshot;
      }

      if (this.config.pluginSystem.disabled) {
        return {
          ...snapshot,
          disableState: { disabled: true, reason: "plugin-system-disabled" },
        };
      }

      if (
        snapshot.origin === "community" &&
        this.config.pluginSystem.communityDisabled
      ) {
        return {
          ...snapshot,
          disableState: {
            disabled: true,
            reason: "community-plugins-disabled",
          },
        };
      }

      if (this.config.plugins?.[snapshot.manifest.id]?.disabled) {
        return {
          ...snapshot,
          disableState: { disabled: true, reason: "disabled-by-config" },
        };
      }

      return snapshot;
    });
  }
}
