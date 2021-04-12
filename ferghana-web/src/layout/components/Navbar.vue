<template>
  <div class="navbar el-button--primary">
<!--    <hamburger id="hamburger-container" :is-active="sidebar.opened"-->
<!--                class="hamburger-container" @toggleClick="toggleSideBar" />-->

    <breadcrumb id="breadcrumb-container" class="breadcrumb-container" />


    <div class="right-menu">
      <template v-if="device!=='mobile'">
<!--        <search id="header-search" class="right-menu-item" />-->
<!--        -->
<!--        <el-tooltip content="源码地址" effect="dark" placement="bottom">-->
<!--          <ruo-yi-git id="ruoyi-git" class="right-menu-item hover-effect" />-->
<!--        </el-tooltip>-->

<!--        <el-tooltip content="文档地址" effect="dark" placement="bottom">-->
<!--          <ruo-yi-doc id="ruoyi-doc" class="right-menu-item hover-effect" />-->
<!--        </el-tooltip>-->

<!--        <screenfull id="screenfull" class="right-menu-item hover-effect" />-->

        <el-tooltip content="布局大小" effect="dark" placement="bottom">

        </el-tooltip>

      </template>

      <el-dropdown class="avatar-container right-menu-item hover-effect" trigger="click">


        <div class="avatar-wrapper">
<!--          用户头像-->
          <img :src="avatar" class="user-avatar">
<!--          <i class="el-icon-caret-bottom" />-->
        </div>
        <el-dropdown-menu slot="dropdown">
          <router-link to="/user/profile">
            <el-dropdown-item>个人中心</el-dropdown-item>
          </router-link>
          <el-dropdown-item>
            <span @click="setting = true">布局设置</span>
          </el-dropdown-item>
          <div style="padding-left: 18px;padding-top: 3px;font-size: 12px">
            <size-select id="size-select" class="right-menu-item hover-effect" />
          </div>
          <el-dropdown-item divided>
            <span @click="logout">退出登录</span>
          </el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
    </div>

    <div class="right-menu">
      <span>{{name}}，欢迎登录！</span>
    </div>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import Breadcrumb from '@/components/Breadcrumb'
import Hamburger from '@/components/Hamburger'
import Screenfull from '@/components/Screenfull'
import SizeSelect from '@/components/SizeSelect'
import Search from '@/components/HeaderSearch'
import RuoYiGit from '@/components/RuoYi/Git'
import RuoYiDoc from '@/components/RuoYi/Doc'
import '@/assets/styles/element-variables.scss';

export default {
  components: {
    Breadcrumb,
    Hamburger,
    Screenfull,
    SizeSelect,
    Search,
    RuoYiGit,
    RuoYiDoc
  },
  computed: {
    ...mapGetters([
      'sidebar',
      'avatar',
      'name',
      'device'
    ]),
    setting: {
      get() {
        return this.$store.state.settings.showSettings
      },
      set(val) {
        this.$store.dispatch('settings/changeSetting', {
          key: 'showSettings',
          value: val
        })
      }
    }
  },
  methods: {
    toggleSideBar() {
      this.$store.dispatch('app/toggleSideBar')
    },
    async logout() {
      this.$confirm('确定注销并退出系统吗？', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.$store.dispatch('LogOut').then(() => {
          location.reload()
        })
      })
    }
  }
}
</script>

<style lang="scss" scoped>
.navbar {
  height: 50px;
  overflow: hidden;
  position: relative;
  background: -moz-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -webkit-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -o-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: -ms-linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  background: linear-gradient(top,#00a0f1,#0075bd,#005d9f);
  box-shadow: 0 1px 4px rgba(0,21,41,.08);
  .hamburger-container {
    line-height: 46px;
    height: 100%;
    float: left;
    cursor: pointer;
    transition: background .3s;
    /*-webkit-tap-highlight-color:transparent;*/
    color: beige;

    &:hover {
      background: rgba(0, 0, 0, 0);
    }
  }

  /*background-color: #409EFF;*/

  .breadcrumb-container {
    float: left;
    margin-left: 20px;
  }

  .errLog-container {
    display: inline-block;
    vertical-align: top;
  }

  .right-menu {
    float: right;
    height: 100%;
    line-height: 50px;

    &:focus {
      outline: none;
    }
    .right-menu-item {
      display: inline-block;
      padding: 0 8px;
      height: 100%;
      font-size: 14px;
      color: white;
      vertical-align: text-bottom;

      &.hover-effect {
        cursor: pointer;
        transition: background .3s;

        &:hover {
          background: rgba(0, 0, 0, .025)
        }
      }
    }

    .avatar-container {
      margin-right: 10px;

      .avatar-wrapper {
        margin-top: 5px;
        position: relative;

        .user-avatar {
          cursor: pointer;
          width: 40px;
          height: 40px;
          border-radius: 10px;
        }

        .el-icon-caret-bottom {
          cursor: pointer;
          position: absolute;
          right: -20px;
          top: 25px;
          font-size: 14px;
        }
      }
    }
  }
}
</style>
