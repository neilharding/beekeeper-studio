import type PluginManager from "./PluginManager";
import type { PluginSnapshot } from "./types";

export type PluginSourcePathParams = {
  path: string;
  id: string;
  cleanupAfterInstall: boolean;
};

export interface ModuleHookMap {
  "before-initialize": () => void | Promise<void>;
  "before-install-plugin": (pluginId: string) => void | Promise<void>;
  "plugin-snapshots": (
    snapshots: PluginSnapshot[]
  ) => PluginSnapshot[] | Promise<PluginSnapshot[]>;
  "plugin-source": (
    params: PluginSourcePathParams
  ) => PluginSourcePathParams | Promise<PluginSourcePathParams>;
}

export type ModuleHook = {
  [K in keyof ModuleHookMap]: {
    name: K;
    handler: ModuleHookMap[K];
  };
}[keyof ModuleHookMap];

export type ModuleOptions = {
  manager: PluginManager;
};

export abstract class Module {
  manager: PluginManager;
  private _hooks: ModuleHook[] = [];

  constructor(options: ModuleOptions) {
    this.manager = options.manager;
  }

  /**
   * Register a handler to run during a lifecycle hook.
   */
  protected hook<K extends keyof ModuleHookMap>(
    name: K,
    handler: ModuleHookMap[K]
  ) {
    this._hooks.push({ name, handler: handler.bind(this) } as ModuleHook);
  }

  get hooks(): ReadonlyArray<ModuleHook> {
    return this._hooks;
  }

  protected getModule<T extends Module>(cls: ModuleClass<T>): T | undefined {
    return this.manager.modules.find((module) => module instanceof cls) as T;
  }
}

export type ModuleClass<T extends Module> = new (options: ModuleOptions) => T;
