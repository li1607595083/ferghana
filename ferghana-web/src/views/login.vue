<!--<template>-->
<!--  <div class="login">-->
<!--    <el-form ref="loginForm" :model="loginForm" :rules="loginRules" class="login-form">-->
<!--      <h3 class="title">实时智能魔方1</h3>-->
<!--      <el-form-item prop="username">-->
<!--        <el-input v-model="loginForm.username" type="text" auto-complete="off" placeholder="账号">-->
<!--          <svg-icon slot="prefix" icon-class="user" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--      </el-form-item>-->
<!--      <el-form-item prop="password">-->
<!--        <el-input-->
<!--          v-model="loginForm.password"-->
<!--          type="password"-->
<!--          auto-complete="off"-->
<!--          placeholder="密码"-->
<!--          @keyup.enter.native="handleLogin"-->
<!--        >-->
<!--          <svg-icon slot="prefix" icon-class="password" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--      </el-form-item>-->
<!--      <el-form-item prop="code">-->
<!--        <el-input-->
<!--          v-model="loginForm.code"-->
<!--          auto-complete="off"-->
<!--          placeholder="验证码"-->
<!--          style="width: 63%"-->
<!--          @keyup.enter.native="handleLogin"-->
<!--        >-->
<!--          <svg-icon slot="prefix" icon-class="validCode" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--        <div class="login-code">-->
<!--          <img :src="codeUrl" @click="getCode"/>-->
<!--        </div>-->
<!--      </el-form-item>-->
<!--      <el-checkbox v-model="loginForm.rememberMe" style="margin:0px 0px 25px 0px;">记住密码</el-checkbox>-->
<!--      <el-form-item style="width:100%;">-->
<!--        <el-button-->
<!--          :loading="loading"-->
<!--          size="medium"-->
<!--          type="primary"-->
<!--          style="width:100%;"-->
<!--          @click.native.prevent="handleLogin"-->
<!--        >-->
<!--          <span v-if="!loading">登 录</span>-->
<!--          <span v-else>登 录 中...</span>-->
<!--        </el-button>-->
<!--      </el-form-item>-->
<!--    </el-form>-->
<!--    &lt;!&ndash;  底部  &ndash;&gt;-->
<!--    &lt;!&ndash;    <div class="el-login-footer">&ndash;&gt;-->
<!--    &lt;!&ndash;      <span>Copyright © 2020 BCP All Rights Reserved.</span>&ndash;&gt;-->
<!--    &lt;!&ndash;    </div>&ndash;&gt;-->
<!--  </div>-->
<!--</template>-->

<!--<script>-->
<!--  import {getCodeImg} from "@/api/login";-->
<!--  import Cookies from "js-cookie";-->
<!--  import {encrypt, decrypt} from '@/utils/jsencrypt'-->

<!--  export default {-->
<!--    name: "Login",-->
<!--    data() {-->
<!--      return {-->
<!--        codeUrl: "",-->
<!--        cookiePassword: "",-->
<!--        loginForm: {-->
<!--          username: "admin",-->
<!--          password: "admin123",-->
<!--          rememberMe: false,-->
<!--          code: "",-->
<!--          uuid: ""-->
<!--        },-->
<!--        loginRules: {-->
<!--          username: [-->
<!--            {required: true, trigger: "blur", message: "用户名不能为空"}-->
<!--          ],-->
<!--          password: [-->
<!--            {required: true, trigger: "blur", message: "密码不能为空"}-->
<!--          ],-->
<!--          code: [{required: true, trigger: "change", message: "验证码不能为空"}]-->
<!--        },-->
<!--        loading: false,-->
<!--        redirect: undefined-->
<!--      };-->
<!--    },-->
<!--    watch: {-->
<!--      $route: {-->
<!--        handler: function (route) {-->
<!--          this.redirect = route.query && route.query.redirect;-->
<!--        },-->
<!--        immediate: true-->
<!--      }-->
<!--    },-->
<!--    created() {-->
<!--      this.getCode();-->
<!--      this.getCookie();-->
<!--    },-->
<!--    methods: {-->
<!--      getCode() {-->
<!--        getCodeImg().then(res => {-->
<!--          this.codeUrl = "data:image/gif;base64," + res.img;-->
<!--          this.loginForm.uuid = res.uuid;-->
<!--        });-->
<!--      },-->
<!--      getCookie() {-->
<!--        const username = Cookies.get("username");-->
<!--        const password = Cookies.get("password");-->
<!--        const rememberMe = Cookies.get('rememberMe')-->
<!--        this.loginForm = {-->
<!--          username: username === undefined ? this.loginForm.username : username,-->
<!--          password: password === undefined ? this.loginForm.password : decrypt(password),-->
<!--          rememberMe: rememberMe === undefined ? false : Boolean(rememberMe)-->
<!--        };-->
<!--      },-->
<!--      handleLogin() {-->
<!--        this.$refs.loginForm.validate(valid => {-->
<!--          if (valid) {-->
<!--            this.loading = true;-->
<!--            if (this.loginForm.rememberMe) {-->
<!--              Cookies.set("username", this.loginForm.username, {expires: 30});-->
<!--              Cookies.set("password", encrypt(this.loginForm.password), {expires: 30});-->
<!--              Cookies.set('rememberMe', this.loginForm.rememberMe, {expires: 30});-->
<!--            } else {-->
<!--              Cookies.remove("username");-->
<!--              Cookies.remove("password");-->
<!--              Cookies.remove('rememberMe');-->
<!--            }-->
<!--            this.$store-->
<!--              .dispatch("Login", this.loginForm)-->
<!--              .then(() => {-->
<!--                this.$router.push({path: this.redirect || "/"});-->
<!--              })-->
<!--              .catch(() => {-->
<!--                this.loading = false;-->
<!--                this.getCode();-->
<!--              });-->
<!--          }-->
<!--        });-->
<!--      }-->
<!--    }-->
<!--  };-->
<!--</script>-->

<!--<style rel="stylesheet/scss" lang="scss">-->
<!--  .login {-->
<!--    display: flex;-->
<!--    justify-content: center;-->
<!--    align-items: center;-->
<!--    height: 100%;-->
<!--    background-image: url("../assets/image/login-back-image.jpg");-->
<!--    background-size: cover;-->
<!--  }-->

<!--  .title {-->
<!--    margin: 0px auto 30px auto;-->
<!--    text-align: center;-->
<!--    color: #707070;-->
<!--  }-->

<!--  .login-form {-->
<!--    border-radius: 6px;-->
<!--    background: #ffffff;-->
<!--    width: 400px;-->
<!--    padding: 25px 25px 5px 25px;-->

<!--    .el-input {-->
<!--      height: 38px;-->

<!--      input {-->
<!--        height: 38px;-->
<!--      }-->
<!--    }-->

<!--    .input-icon {-->
<!--      height: 39px;-->
<!--      width: 14px;-->
<!--      margin-left: 2px;-->
<!--    }-->
<!--  }-->

<!--  .login-tip {-->
<!--    font-size: 13px;-->
<!--    text-align: center;-->
<!--    color: #bfbfbf;-->
<!--  }-->

<!--  .login-code {-->
<!--    width: 33%;-->
<!--    height: 38px;-->
<!--    float: right;-->

<!--    img {-->
<!--      cursor: pointer;-->
<!--      vertical-align: middle;-->
<!--    }-->
<!--  }-->

<!--  .el-login-footer {-->
<!--    height: 40px;-->
<!--    line-height: 40px;-->
<!--    position: fixed;-->
<!--    bottom: 0;-->
<!--    width: 100%;-->
<!--    text-align: center;-->
<!--    color: #fff;-->
<!--    font-family: Arial;-->
<!--    font-size: 12px;-->
<!--    letter-spacing: 1px;-->
<!--  }-->
<!--</style>-->
<!--<template>-->
<!--  <div class="login">-->
<!--    <el-form ref="loginForm" :model="loginForm" :rules="loginRules" class="login-form">-->
<!--      <h3 class="title">实时智能魔方1</h3>-->
<!--      <el-form-item prop="username">-->
<!--        <el-input v-model="loginForm.username" type="text" auto-complete="off" placeholder="账号">-->
<!--          <svg-icon slot="prefix" icon-class="user" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--      </el-form-item>-->
<!--      <el-form-item prop="password">-->
<!--        <el-input-->
<!--          v-model="loginForm.password"-->
<!--          type="password"-->
<!--          auto-complete="off"-->
<!--          placeholder="密码"-->
<!--          @keyup.enter.native="handleLogin"-->
<!--        >-->
<!--          <svg-icon slot="prefix" icon-class="password" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--      </el-form-item>-->
<!--      <el-form-item prop="code">-->
<!--        <el-input-->
<!--          v-model="loginForm.code"-->
<!--          auto-complete="off"-->
<!--          placeholder="验证码"-->
<!--          style="width: 63%"-->
<!--          @keyup.enter.native="handleLogin"-->
<!--        >-->
<!--          <svg-icon slot="prefix" icon-class="validCode" class="el-input__icon input-icon"/>-->
<!--        </el-input>-->
<!--        <div class="login-code">-->
<!--          <img :src="codeUrl" @click="getCode"/>-->
<!--        </div>-->
<!--      </el-form-item>-->
<!--      <el-checkbox v-model="loginForm.rememberMe" style="margin:0px 0px 25px 0px;">记住密码</el-checkbox>-->
<!--      <el-form-item style="width:100%;">-->
<!--        <el-button-->
<!--          :loading="loading"-->
<!--          size="medium"-->
<!--          type="primary"-->
<!--          style="width:100%;"-->
<!--          @click.native.prevent="handleLogin"-->
<!--        >-->
<!--          <span v-if="!loading">登 录</span>-->
<!--          <span v-else>登 录 中...</span>-->
<!--        </el-button>-->
<!--      </el-form-item>-->
<!--    </el-form>-->
<!--    &lt;!&ndash;  底部  &ndash;&gt;-->
<!--    &lt;!&ndash;    <div class="el-login-footer">&ndash;&gt;-->
<!--    &lt;!&ndash;      <span>Copyright © 2020 BCP All Rights Reserved.</span>&ndash;&gt;-->
<!--    &lt;!&ndash;    </div>&ndash;&gt;-->
<!--  </div>-->
<!--</template>-->

<!--<script>-->
<!--  import {getCodeImg} from "@/api/login";-->
<!--  import Cookies from "js-cookie";-->
<!--  import {encrypt, decrypt} from '@/utils/jsencrypt'-->

<!--  export default {-->
<!--    name: "Login",-->
<!--    data() {-->
<!--      return {-->
<!--        codeUrl: "",-->
<!--        cookiePassword: "",-->
<!--        loginForm: {-->
<!--          username: "admin",-->
<!--          password: "admin123",-->
<!--          rememberMe: false,-->
<!--          code: "",-->
<!--          uuid: ""-->
<!--        },-->
<!--        loginRules: {-->
<!--          username: [-->
<!--            {required: true, trigger: "blur", message: "用户名不能为空"}-->
<!--          ],-->
<!--          password: [-->
<!--            {required: true, trigger: "blur", message: "密码不能为空"}-->
<!--          ],-->
<!--          code: [{required: true, trigger: "change", message: "验证码不能为空"}]-->
<!--        },-->
<!--        loading: false,-->
<!--        redirect: undefined-->
<!--      };-->
<!--    },-->
<!--    watch: {-->
<!--      $route: {-->
<!--        handler: function (route) {-->
<!--          this.redirect = route.query && route.query.redirect;-->
<!--        },-->
<!--        immediate: true-->
<!--      }-->
<!--    },-->
<!--    created() {-->
<!--      this.getCode();-->
<!--      this.getCookie();-->
<!--    },-->
<!--    methods: {-->
<!--      getCode() {-->
<!--        getCodeImg().then(res => {-->
<!--          this.codeUrl = "data:image/gif;base64," + res.img;-->
<!--          this.loginForm.uuid = res.uuid;-->
<!--        });-->
<!--      },-->
<!--      getCookie() {-->
<!--        const username = Cookies.get("username");-->
<!--        const password = Cookies.get("password");-->
<!--        const rememberMe = Cookies.get('rememberMe')-->
<!--        this.loginForm = {-->
<!--          username: username === undefined ? this.loginForm.username : username,-->
<!--          password: password === undefined ? this.loginForm.password : decrypt(password),-->
<!--          rememberMe: rememberMe === undefined ? false : Boolean(rememberMe)-->
<!--        };-->
<!--      },-->
<!--      handleLogin() {-->
<!--        this.$refs.loginForm.validate(valid => {-->
<!--          if (valid) {-->
<!--            this.loading = true;-->
<!--            if (this.loginForm.rememberMe) {-->
<!--              Cookies.set("username", this.loginForm.username, {expires: 30});-->
<!--              Cookies.set("password", encrypt(this.loginForm.password), {expires: 30});-->
<!--              Cookies.set('rememberMe', this.loginForm.rememberMe, {expires: 30});-->
<!--            } else {-->
<!--              Cookies.remove("username");-->
<!--              Cookies.remove("password");-->
<!--              Cookies.remove('rememberMe');-->
<!--            }-->
<!--            this.$store-->
<!--              .dispatch("Login", this.loginForm)-->
<!--              .then(() => {-->
<!--                this.$router.push({path: this.redirect || "/"});-->
<!--              })-->
<!--              .catch(() => {-->
<!--                this.loading = false;-->
<!--                this.getCode();-->
<!--              });-->
<!--          }-->
<!--        });-->
<!--      }-->
<!--    }-->
<!--  };-->
<!--</script>-->

<!--<style rel="stylesheet/scss" lang="scss">-->
<!--  .login {-->
<!--    display: flex;-->
<!--    justify-content: center;-->
<!--    align-items: center;-->
<!--    height: 100%;-->
<!--    background-image: url("../assets/image/login-back-image.jpg");-->
<!--    background-size: cover;-->
<!--  }-->

<!--  .title {-->
<!--    margin: 0px auto 30px auto;-->
<!--    text-align: center;-->
<!--    color: #707070;-->
<!--  }-->

<!--  .login-form {-->
<!--    border-radius: 6px;-->
<!--    background: #ffffff;-->
<!--    width: 400px;-->
<!--    padding: 25px 25px 5px 25px;-->

<!--    .el-input {-->
<!--      height: 38px;-->

<!--      input {-->
<!--        height: 38px;-->
<!--      }-->
<!--    }-->

<!--    .input-icon {-->
<!--      height: 39px;-->
<!--      width: 14px;-->
<!--      margin-left: 2px;-->
<!--    }-->
<!--  }-->

<!--  .login-tip {-->
<!--    font-size: 13px;-->
<!--    text-align: center;-->
<!--    color: #bfbfbf;-->
<!--  }-->

<!--  .login-code {-->
<!--    width: 33%;-->
<!--    height: 38px;-->
<!--    float: right;-->

<!--    img {-->
<!--      cursor: pointer;-->
<!--      vertical-align: middle;-->
<!--    }-->
<!--  }-->

<!--  .el-login-footer {-->
<!--    height: 40px;-->
<!--    line-height: 40px;-->
<!--    position: fixed;-->
<!--    bottom: 0;-->
<!--    width: 100%;-->
<!--    text-align: center;-->
<!--    color: #fff;-->
<!--    font-family: Arial;-->
<!--    font-size: 12px;-->
<!--    letter-spacing: 1px;-->
<!--  }-->
<!--</style>-->
<!--=======-->
<template>
  <div class="login">
    <el-form ref="loginForm" :model="loginForm" :rules="loginRules" class="login-form el-col-24" >
<!--        <img  v-if="logo" :src="logo">-->
      <el-form-item class="el-col-24">
        <img  v-if="logo" :src="logo" class="el-col-4" style="width:200px;margin-top: 12px;margin-left: 40px">
        <span class="title el-col-8" >实时智能魔方</span>
      </el-form-item>
      <el-form-item prop="username" class="el-col-24">
        <el-input v-model="loginForm.username" type="text" auto-complete="off" placeholder="账号">
          <svg-icon slot="prefix" icon-class="user" class="el-input__icon input-icon"/>
        </el-input>
      </el-form-item>
      <el-form-item prop="password" class="el-col-24">
        <el-input
          v-model="loginForm.password"
          type="password"
          auto-complete="off"
          placeholder="密码"
          @keyup.enter.native="handleLogin"
        >
          <svg-icon slot="prefix" icon-class="password" class="el-input__icon input-icon"/>
        </el-input>
      </el-form-item>
      <!--<el-form-item prop="code">-->
      <!--<el-input-->
      <!--v-model="loginForm.code"-->
      <!--auto-complete="off"-->
      <!--placeholder="验证码"-->
      <!--style="width: 63%"-->
      <!--@keyup.enter.native="handleLogin"-->
      <!--&gt;-->
      <!--<svg-icon slot="prefix" icon-class="validCode" class="el-input__icon input-icon" />-->
      <!--</el-input>-->
      <!--<div class="login-code">-->
      <!--<img :src="codeUrl" @click="getCode" />-->
      <!--</div>-->
      <!--</el-form-item>-->
      <el-checkbox v-model="loginForm.rememberMe" style="margin:0px 0px 25px 0px;">记住密码</el-checkbox>
      <el-form-item style="width:100%;">
        <el-button
          :loading="loading"
          size="medium"
          type="primary"
          style="width:100%;"
          @click.native.prevent="handleLogin"
        >
          <span v-if="!loading">登 录</span>
          <span v-else>登 录 中...</span>
        </el-button>
      </el-form-item>
    </el-form>
    <!--  底部  -->
     <div class="el-login-footer">
       <span>Copyright © 1999-2021 上海诺祺科技有限公司. </span>
     </div>
  </div>
</template>

<script>
  import {getCodeImg} from "@/api/login";
  import Cookies from "js-cookie";
  import {encrypt, decrypt} from '@/utils/jsencrypt';
  // import logoImg from '@/assets/logo/logo2.gif'
  import logoImg from '@/assets/logo/logo2.gif'

  export default {
    name: "Login",
    data() {
      return {
        logo: logoImg,
        codeUrl: "",
        cookiePassword: "",
        loginForm: {
          username: "admin",
          password: "admin123",
          rememberMe: false,
          code: "",
          uuid: ""
        },
        loginRules: {
          username: [
            {required: true, trigger: "blur", message: "用户名不能为空"}
          ],
          password: [
            {required: true, trigger: "blur", message: "密码不能为空"}
          ],
          code: [{required: true, trigger: "change", message: "验证码不能为空"}]
        },
        loading: false,
        redirect: undefined
      };
    },
    watch: {
      $route: {
        handler: function (route) {
          this.redirect = route.query && route.query.redirect;
        },
        immediate: true
      }
    },
    created() {
      this.getCode();
      this.getCookie();
    },
    methods: {
      getCode() {
        getCodeImg().then(res => {
          this.codeUrl = "data:image/gif;base64," + res.img;
          this.loginForm.uuid = res.uuid;
        });
      },
      getCookie() {
        const username = Cookies.get("username");
        const password = Cookies.get("password");
        const rememberMe = Cookies.get('rememberMe')
        this.loginForm = {
          username: username === undefined ? this.loginForm.username : username,
          password: password === undefined ? this.loginForm.password : decrypt(password),
          rememberMe: rememberMe === undefined ? false : Boolean(rememberMe)
        };
      },
      handleLogin() {
        this.$refs.loginForm.validate(valid => {
          if (valid) {
            this.loading = true;
            if (this.loginForm.rememberMe) {
              Cookies.set("username", this.loginForm.username, {expires: 30});
              Cookies.set("password", encrypt(this.loginForm.password), {expires: 30});
              Cookies.set('rememberMe', this.loginForm.rememberMe, {expires: 30});
            } else {
              Cookies.remove("username");
              Cookies.remove("password");
              Cookies.remove('rememberMe');
            }
            this.$store
              .dispatch("Login", this.loginForm)
              .then(() => {
                this.$router.push({path: this.redirect || "/"});
              })
              .catch(() => {
                this.loading = false;
                this.getCode();
              });
          }
        });
      }
    }
  };
</script>

<style rel="stylesheet/scss" lang="scss">
  .login {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100%;
    background-image: url("../assets/image/login_back.jpg");
    background-size: cover;
  }

  .title {
    margin: 19px auto 0px auto;
    margin-left: 10px;
    text-align: center;
    /*color: #707070;*/
    color: #185093;
    font-size: 21px;
    letter-spacing: 2px;
    font-family: ChuQianTi;
    opacity: 0.9;
  }
  .el-button--primary{
    background-color: #0083CB;
  }

  .login-form {
    border-radius: 6px;
    background: #ffffff;
    width: 480px;
    padding: 25px 25px 5px 25px;

    .el-input {
      height: 38px;

      input {
        height: 38px;
      }
    }

    .input-icon {
      height: 39px;
      width: 14px;
      margin-left: 2px;
    }
  }

  .login-tip {
    font-size: 13px;
    text-align: center;
    color: #bfbfbf;
  }

  .login-code {
    width: 33%;
    height: 38px;
    float: right;

    img {
      cursor: pointer;
      vertical-align: middle;
    }
  }

  .el-login-footer {
    height: 40px;
    line-height: 40px;
    position: fixed;
    bottom: 0;
    width: 100%;
    text-align: center;
    color: #f5f5f5;
    font-family: Arial;
    font-size: 15px;
    letter-spacing: 1px;
  }
</style>
