<template>
  <div>
  </div>
</template>

<script>

  export default {
    name: "SSOLogin",
    watch: {},
    created() {
      let userAccount = this.$route.query.userAccount;
      let loginToken = this.$route.query.loginToken;
      this.redirect = this.$route.query.redirect;
      let ssoTokenData = {
        userAccount: userAccount,
        loginToken: loginToken,
      }
      this.$store.dispatch("SSOLogin", ssoTokenData).then(() => {
        this.$router.push({
          path: this.redirect || "/"
        });
      }).catch(() => {
      })
    },
    methods: {}
  };
</script>

<style rel="stylesheet/scss" lang="scss">
</style>



