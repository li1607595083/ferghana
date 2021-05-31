<template>
  <div>
    <el-scrollbar wrap-class="scrollbar-wrapper" v-if="!menuLayout">
      <el-menu
        :default-active="activeMenu"
        :collapse="isCollapse"
        :background-color="variables.menuBg"
        :text-color="variables.menuText"
        :unique-opened="true"
        :active-text-color="variables.menuActiveText"
        :collapse-transition="false"
        mode="vertical"
      >
        <sidebar-item v-for="route in permission_routes" :key="route.path" :item="route" :base-path="route.path" :horizontal="menuLayout"/>
      </el-menu>
    </el-scrollbar>
   <el-menu v-else
      :default-active="activeMenu"
      :text-color="variables.menuTextHorizontal"
      :unique-opened="true"
      :active-text-color="variables.menuActiveTextHorizontal"
      :collapse-transition="false"
      mode="horizontal"
      style="display: flex;"
    >
      <sidebar-item v-for="route in permission_routes" :key="route.path" :item="route" :base-path="route.path" :horizontal="menuLayout"/>
    </el-menu>
  </div>
</template>

<script>
import { mapGetters, mapState } from 'vuex'
import SidebarItem from './SidebarItem'
import variables from '@/assets/styles/variables.scss'

export default {
  components: { SidebarItem },
  computed: {
    ...mapGetters([
      'permission_routes',
      'sidebar',
    ]),
    ...mapState({
      menuLayout: state => state.settings.menuLayout
    }),
    activeMenu() {
      const route = this.$route
      const { meta, path } = route
      // if set path, the sidebar will highlight the path you set
      if (meta.activeMenu) {
        return meta.activeMenu
      }
      return path
    },
    variables() {
      return variables
    },
    isCollapse() {
      return !this.sidebar.opened
    }
  }
}
</script>

<style>
  .el-menu {
      background-color: rgba(0,0,0,0) !important;
      border-right: solid 1px rgba(0,0,0,0) !important;
  }
</style>
