<template>
  <div class="sidebar-logo-container el-button--primary" :class="{'collapse':collapse}">
    <transition name="sidebarLogoFade">
      <router-link v-if="collapse" key="collapse" to="/">
        <img v-if="logo" :src="logo">
<!--        <h1 v-else class="sidebar-title">{{ title }} </h1>-->
      </router-link>
      <router-link v-else key="expand" to="/">
        <img v-if="logo" style="width: 190px;padding-top: 3px"  :src="logo">
<!--        <h1 class="sidebar-title" style="font-size: 20px">{{ title }} </h1>-->
      </router-link>
    </transition>
  </div>
</template>

<script>
// import logoImg from '@/assets/logo/274_58.gif'
import logoImg from '@/assets/logo/indexLogin.gif'
// import logoImg from '@/assets/logo/logoss.gif'

export default {
  name: 'SidebarLogo',
  props: {
    collapse: {
      type: Boolean,
      required: true
    }
  },
  data() {
    return {
      title: '实时智能魔方',
      logo: logoImg
    }
  }
}
</script>

<style lang="scss" scoped>
.sidebarLogoFade-enter-active {
  transition: opacity 1.5s;
}

.sidebarLogoFade-enter,
.sidebarLogoFade-leave-to {
  opacity: 0;
}

.sidebar-logo-container {
  position: relative;
  width: 100%;
  height: 50px;
  line-height: 50px;
  background: -moz-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -webkit-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -o-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -ms-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  text-align: center;
  overflow: hidden;

  & .sidebar-logo-link {
    height: 100%;
    width: 100%;

    & .sidebar-logo {
      width: 200px;
      height: 42px;
      vertical-align: middle;
    }

    & .sidebar-title {
      display: inline-block;
      margin: 0;
      color: #fff;
      font-weight: 600;
      line-height: 50px;
      font-size: 14px;
      font-family: Avenir, Helvetica Neue, Arial, Helvetica, sans-serif;
      vertical-align: middle;
    }
  }

  &.collapse {
    .sidebar-logo {
      margin-right: 0px;
    }
  }

}
</style>
