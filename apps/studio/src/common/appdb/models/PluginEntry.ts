import { Column, Entity } from "typeorm";
import { ApplicationEntity } from "./application_entity";
import { PluginOrigin, PluginRegistryEntry } from "@/services/plugin/types";

@Entity({ name: "plugin_entries" })
export class PluginEntry extends ApplicationEntity {
  withProps(props?: any): PluginEntry {
    if (props) PluginEntry.merge(this, props);
    return this;
  }

  @Column({ type: "varchar", nullable: false })
  pluginId: string;

  @Column({ type: "varchar", nullable: false })
  name: string;

  @Column({ type: "varchar", nullable: false })
  author: string;

  @Column({ type: "varchar", nullable: false })
  authorUrl: string;

  @Column({ type: "varchar", nullable: false })
  repo: string;

  @Column({ type: "text", nullable: false })
  description: string;

  @Column({
    type: "varchar",
    nullable: false,
    enum: ["core", "community", "unlisted"],
    default: "unlisted",
  })
  origin: PluginOrigin;
}
