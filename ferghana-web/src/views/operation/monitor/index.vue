<template>
  <div class="app-container">
    <div class="card-container">
      <el-card class="card-box" v-for="(item, index) in cardData" :key="index">
        <div class="card-head">{{item.name}}</div>
        <div class="card-content">
          <div v-for="(subItem, subIndex) in item.content" :key="sunIndex" class="card-sub-content">
            <div class="card-sub-head">{{subItem.name}}</div>
            <div class="card-sub-value">
              <span :style="{color: subItem.color}">{{subItem.value}}</span>
              <span class="card-sub-unit">{{item.unit}}</span>
            </div>
          </div>
        </div>
      </el-card>
    </div>

    <div class="monitor-map">
      <el-card class="monitor-map-card">
        <ve-line :data="chartData" style="width: 100%;"></ve-line>
      </el-card>
      <el-card class="monitor-map-card">
        <ve-line :data="chartData" style="width: 100%;"></ve-line>
      </el-card>
    </div>
    
    <div class="monitor-map">
      <el-card class="monitor-map-card">
        <ve-line :data="chartData" style="width: 100%;" :extend="extend"></ve-line>
      </el-card>
      <el-card class="monitor-map-card">
        <ve-line :data="chartData" style="width: 100%;" :extend="extend"></ve-line>
      </el-card>
    </div>
  </div>
</template>

<script>
  export default {
    name: "Monitor",
    data() {
      return {
        extend: {
          series: {
            smooth: false
          }
        },
        chartData: {
          columns: ["日期", "访问用户", "下单用户", "下单率"],
          rows: [{
              日期: "1/1",
              访问用户: 1393,
              下单用户: 1093,
              下单率: 0.32
            },
            {
              日期: "1/2",
              访问用户: 3530,
              下单用户: 3230,
              下单率: 0.26
            },
            {
              日期: "1/3",
              访问用户: 2923,
              下单用户: 2623,
              下单率: 0.76
            },
            {
              日期: "1/4",
              访问用户: 1723,
              下单用户: 1423,
              下单率: 0.49
            },
            {
              日期: "1/5",
              访问用户: 3792,
              下单用户: 3492,
              下单率: 0.323
            },
            {
              日期: "1/6",
              访问用户: 4593,
              下单用户: 4293,
              下单率: 0.78
            }
          ]
        },
        cardData: [{
          name: "CPU资源",
          unit: "CU",
          content: [{
            name: "共计",
            value: 0,
            color: "#000000"
          }, {
            name: "使用",
            value: 0,
            color: "#57BB66"
          }, {
            name: "剩余",
            value: 0,
            color: "#EF7930"
          }]
        }, {
          name: "内存资源",
          unit: "GB",
          content: [{
            name: "共计",
            value: 0,
            color: "#000000"
          }, {
            name: "使用",
            value: 0,
            color: "#57BB66"
          }, {
            name: "剩余",
            value: 0,
            color: "#EF7930"
          }]
        }, {
          name: "作业",
          unit: "个",
          content: [{
            name: "运行中",
            value: 0,
            color: "#57BB66"
          }, {
            name: "停止",
            value: 0,
            color: "#EF7930"
          }]
        }]
      };
    },
    created() {

    },
    methods: {


    }
  }
</script>

<style>
  .card-container {
    width: 100%;
    display: flex;
    justify-content: space-between;
  }

  .card-box {
    width: 100%;
    margin: 0 20px;
    cursor: pointer;
  }

  .card-head {
    width: 100%;
    text-align: left;
    color: #999999;
    font-weight: bold;
  }

  .card-content {
    width: 100%;
    display: flex;
    padding: 20px 0;
    justify-content: space-between;
  }

  .card-sub-content {
    width: 100%;
    margin-left: 30px;
  }

  .card-sub-head {
    width: 100%;
    text-align: left;
    font-weight: bold;
    color: #000000;
  }

  .card-sub-value {
    font-size: 45px;
    margin-top: 15px;
    font-weight: bold;
  }

  .card-sub-unit {
    font-size: 20px;
  }

  .monitor-map {
    width: 100%;
    display: flex;
  }

  .monitor-map-card {
    width: 100%;
    margin: 20px;
  }
</style>
