<template>
  <div class="with-connection-type">
    <div class="alert alert-warning">
      <i class="material-icons">warning</i>
      <span>
        Athena support is in alpha.
        <a href="https://github.com/beekeeper-studio/beekeeper-studio/issues/new/choose">Report an issue</a>.
      </span>
    </div>
    <div class="form-group col">
      <label for="authenticationType">Authentication Method</label>
      <select name="" v-model="authType" id="">
        <option :key="`${t.value}`" v-for="t in authTypes" :value="t.value" :selected="authType === t.value">
          {{ t.name }}
        </option>
      </select>
    </div>
    <div class="form-group">
      <label for="defaultDatabase">Default Catalog</label>
      <input
        type="text"
        class="form-control"
        v-model="config.defaultDatabase"
        placeholder="AwsDataCatalog"
      >
    </div>
    <div class="form-group">
      <label for="athenaDatabase">Default Database</label>
      <input
        type="text"
        class="form-control"
        v-model="config.athenaOptions.database"
        placeholder="default"
      >
    </div>
    <div class="form-group">
      <label for="s3OutputLocation">S3 Output Location <span class="hint">(optional if workgroup configured)</span></label>
      <input
        type="text"
        class="form-control"
        v-model="config.athenaOptions.s3OutputLocation"
        placeholder="s3://my-bucket/athena-results/"
      >
    </div>
    <div class="form-group">
      <label for="workgroup">Workgroup <span class="hint">(optional)</span></label>
      <input
        type="text"
        class="form-control"
        v-model="config.athenaOptions.workgroup"
        placeholder="primary"
      >
    </div>
    <common-iam v-show="iamAuthenticationEnabled" :config="config" :auth-type="authType" />
  </div>
</template>
<script>

import { IamAuthTypes } from "@/lib/db/types";
import CommonIam from "@/components/connection/CommonIam.vue";

export default {
  components: { CommonIam },
  data() {
    return {
      iamAuthenticationEnabled: this.config.iamAuthOptions?.authType?.includes?.('iam'),
      authType: this.config.iamAuthOptions?.authType || 'default',
      authTypes: [{ name: 'Username / Password', value: 'default' }, ...IamAuthTypes]
    }
  },
  watch: {
    async authType() {
      if (this.authType.includes('iam')) {
        this.iamAuthenticationEnabled = true;
        this.config.iamAuthOptions.authType = this.authType
      } else {
        this.iamAuthenticationEnabled = false;
      }
    },
    iamAuthenticationEnabled() {
      this.config.iamAuthOptions.iamAuthenticationEnabled = this.iamAuthenticationEnabled
    }
  },
  props: ['config'],
  mounted() {
    if (!this.config.athenaOptions) {
      this.$set(this.config, 'athenaOptions', {})
    }
  }
}
</script>
