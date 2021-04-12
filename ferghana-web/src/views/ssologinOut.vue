<template>
  <div>
  </div>
</template>

<script>

  export default {
    name: "SSOLoginOut",
    watch: {},
    created() {
      this.$store.dispatch('LogOut').then(() => {
        // location.reload()
        window.opener = null;
        window.open("about:blank", "_top").close();
      }).catch(() => {

      })
    },
    methods: {}
  };
</script>

<style rel="stylesheet/scss" lang="scss">
</style>



