<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script>
export default  {
  name:  'App'
}
</script>

<style>
  @font-face {
      font-family: PuHuiTi;
      src: url('./assets/fonts/Alibaba-PuHuiTi-Regular.ttf');
  }
  @font-face {
      font-family: ChuQianTi;
      src: url('./assets/fonts/ChuQIanTi.ttf');
  }
  * {
    font-family: PuHuiTi;
  }
</style>
