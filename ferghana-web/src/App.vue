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
      src: url('./assets/fonts/方正粗倩简体.ttf');
  }
  * {
    font-family: PuHuiTi;
  }
</style>
