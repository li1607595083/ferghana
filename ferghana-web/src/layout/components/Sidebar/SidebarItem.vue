<template>
  <div v-if="!item.hidden" class="menu-wrapper" :class="{'horizontal-menu':menuLayout}">
    <template
      v-if="hasOneShowingChild(item.children,item) && (!onlyOneChild.children||onlyOneChild.noShowingChildren)&&!item.alwaysShow">
      <app-link v-if="onlyOneChild.meta" :to="resolvePath(onlyOneChild.path)">
        <el-menu-item :index="resolvePath(onlyOneChild.path)" :class="{'submenu-title-noDropdown':!isNest}">
          <item :icon="onlyOneChild.meta.icon||(item.meta&&item.meta.icon)" :title="onlyOneChild.meta.title" />
        </el-menu-item>
      </app-link>
    </template>

    <el-submenu v-else ref="subMenu" :index="resolvePath(item.path)" popper-append-to-body>
      <template slot="title">
        <item v-if="item.meta" :icon="item.meta && item.meta.icon" :title="item.meta.title" />
      </template>
      <sidebar-item v-for="child in item.children" :key="child.path" :is-nest="true" :item="child"
        :base-path="resolvePath(child.path)" class="nest-menu" />
    </el-submenu>
  </div>
</template>

<script>
  import {
    mapState
  } from 'vuex'
  import path from 'path'
  import {
    isExternal
  } from '@/utils/validate'
  import Item from './Item'
  import AppLink from './Link'
  import FixiOSBug from './FixiOSBug'

  export default {
    name: 'SidebarItem',
    components: {
      Item,
      AppLink
    },
    computed: {
      ...mapState({
        menuLayout: state => state.settings.menuLayout
      })
    },
    mixins: [FixiOSBug],
    props: {
      // route object
      item: {
        type: Object,
        required: true
      },
      isNest: {
        type: Boolean,
        default: false
      },
      basePath: {
        type: String,
        default: ''
      }
    },
    data() {
      this.onlyOneChild = null
      return {}
    },
    methods: {
      hasOneShowingChild(children = [], parent) {
        const showingChildren = children.filter(item => {
          if (item.hidden) {
            return false
          } else {
            // Temp set(will be used if only has one showing child)
            this.onlyOneChild = item
            return true
          }
        })

        // When there is only one child router, the child router is displayed by default
        if (showingChildren.length === 1) {
          return true
        }

        // Show parent if there are no child router to display
        if (showingChildren.length === 0) {
          this.onlyOneChild = {
            ...parent,
            path: '',
            noShowingChildren: true
          }
          return true
        }

        return false
      },
      resolvePath(routePath) {
        if (isExternal(routePath)) {
          return routePath
        }
        if (isExternal(this.basePath)) {
          return this.basePath
        }
        return path.resolve(this.basePath, routePath)
      }
    }
  }
</script>

<style lang="scss" scoped>
  @import "~@/assets/styles/variables.scss";

  .horizontal-menu {

    .el-submenu__title {
      & span {
        margin: 0 10px;
        padding-right: 15px;
        color: $menuTextHorizontal !important;
      }

      & * {
        color: $subMenuTextHorizontal !important;
      }
    }

    &>.el-submenu,
    .el-menu-item {
      &:hover {
        background-color: $menuBgActiveHorizontal !important;
      }

      & :hover {
        background-color: $menuBgActiveHorizontal !important;
      }

      &:hover * {
        background-color: rgba(0, 0, 0, 0) !important;
      }
    }

    &>.el-submenu,
    .el-menu-item {
      & :hover * {
        color: $menuActiveTextHorizontal !important;
      }

      &:hover * {
        color: $menuActiveTextHorizontal !important;
      }
    }

    // & :hover {
    //   background-color: $menuBgActiveHorizontal !important;
    // }

    // & :hover * {
    //   color: $menuActiveTextHorizontal !important;
    // }

    &.horizontal-menu>a>li[class='el-menu-item'] {
      color: $subMenuTextHorizontal !important;
    }

    .el-menu-item>span {
      margin: 0 10px;
    }

    .is-active,
    .is-opened {
      background-color: $menuBgActiveHorizontal !important;
    }

    .is-active * {
      color: $menuActiveTextHorizontal !important;
    }

    .is-opened * {
      color: $menuActiveTextHorizontal !important;
    }

    &>.el-submenu>.el-submenu__title {
      &:hover * {
        color: $subMenuActiveTextHorizontal !important;
      }

      & * {
        color: $menuTextHorizontal !important;
      }
    }

    &>.el-submenu.is-active * {
      color: $menuActiveTextHorizontal !important;
    }

    &>.el-submenu.is-opened * {
      color: $menuActiveTextHorizontal !important;
    }

    &.nest-menu>.el-submenu>.el-submenu__title {
      &:hover * {
        color: $subMenuActiveTextHorizontal !important;
      }

      & * {
        color: $subMenuTextHorizontal !important;
      }
    }

  }
</style>
