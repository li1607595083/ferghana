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

    <div class="card-container">
      <el-card class="card-from">
        <el-row type="flex" justify="space-between">

          <el-row type="flex">
            <div class="card-from-select-box">
              <div class="card-from-select">
                <label class="el-form-item__label">运行中作业</label>
                <el-select v-model="from.packageValue" style="width: 70%;" placeholder="请选择" size="small"
                  @change="loadData">
                  <el-option v-for="(item, index) in packageOptions" :key="index" :label="item.variablePackName"
                    :value="item.jobId"></el-option>
                </el-select>
              </div>
            </div>

            <div class="card-from-select-box">
              <div class="card-from-select">
                <label class="el-form-item__label">时间细粒度</label>
                <el-select v-model="from.granularity" style="width: 70%;" placeholder="请选择" size="small"
                  @change="loadData">
                  <el-option v-for="(item, index) in granularityOptions" :key="index" :label="item.dictLabel"
                    :value="item.dictValue"></el-option>
                </el-select>
              </div>
            </div>
          </el-row>

          <el-row type="flex" align="middle">
            <el-button size="small" icon="el-icon-refresh" @click="loadData">刷新</el-button>
          </el-row>

        </el-row>

      </el-card>
    </div>

    <div class="monitor-map">
      <el-card class="monitor-map-card">
        <ve-line :data="avgComputerDuration" style="width: 100%;" :extend="extend" :loading="chartLoading"
          :data-empty="chartEmpty" :settings="setting_two"></ve-line>
      </el-card>
      <el-card class="monitor-map-card">
        <ve-line :data="numRecordsOut" style="width: 100%;" :extend="extend" :data-empty="chartEmpty"
          :loading="chartLoading" :settings="setting_two"></ve-line>
      </el-card>
    </div>

    <div class="monitor-map">
      <el-card class="monitor-map-card">
        <ve-line :data="jobMangerCpu" style="width: 100%;" :extend="extend" :loading="chartLoading"
          :data-empty="chartEmpty" :settings="setting"></ve-line>
      </el-card>
      <el-card class="monitor-map-card">
        <ve-line :data="jobMangerHead" style="width: 100%;" :extend="extend" :data-empty="chartEmpty"
          :loading="chartLoading" :settings="setting_two"></ve-line>
      </el-card>
    </div>

    <div class="monitor-map">
      <el-card class="monitor-map-card">
        <ve-line :data="taskMangerCpu" style="width: 100%;" :extend="extend" :data-empty="chartEmpty"
          :loading="chartLoading" :settings="setting"></ve-line>
      </el-card>
      <el-card class="monitor-map-card">
        <ve-line :data="taskMangerHead" style="width: 100%;" :loading="chartLoading" :data-empty="chartEmpty"
          :extend="extend" :settings="setting_two"></ve-line>
      </el-card>
    </div>
  </div>
</template>

<script>
  import {
    getInfo,
    getChartInfo,
    getStartInfo,
    dateFormat
  } from '@/api/operation/monitor/index.js'
  export default {
    name: "Monitor",
    data() {
      return {
        extend: {
          // series: {
          //   smooth: false
          // }
          xAxis: {
            splitLine: {
              show: false
            },
            axisLabel: {
              interval: 0,
              formatter: (value) => dateFormat("HH:MM", new Date(value))
            },
            type: 'time',
            minInterval: 3600 * 1000,
            maxInterval: 3600 * 1000,
          },
        },
        chartLoading: false,
        chartEmpty: false,
        setting: {
          yAxisType: ["percent"],
          digit: "2",
          labelMap: {
            jobMangerCpu: "jobMangerCPU使用率",
            taskMangerCpu: "taskMangerCPU使用率"
          }
        },
        setting_two: {
          labelMap: {
            jobMangerHead: "jobManger堆使用率",
            taskMangerHead: "taskManger堆使用率",
            avgComputerDuration: "计算延迟",
            numRecordsOut: "数据输出条数"
          }
        },
        timer: null,
        chartData: {
          columns: [],
          rows: []
        },
        jobMangerHead: null,
        jobMangerCpu: null,
        taskMangerCpu: null,
        taskMangerHead: null,
        avgComputerDuration: null,
        numRecordsOut: null,
        cardData: [{
          name: "集群CPU资源",
          unit: "CU",
          content: [{
            name: "共计",
            value: 0,
            color: "#000000"
          }, {
            name: "使用中",
            value: 0,
            color: "#57BB66"
          }, {
            name: "剩余",
            value: 0,
            color: "#EF7930"
          }]
        }, {
          name: "集群内存资源",
          unit: "GB",
          content: [{
            name: "共计",
            value: 0,
            color: "#000000"
          }, {
            name: "使用中",
            value: 0,
            color: "#57BB66"
          }, {
            name: "剩余",
            value: 0,
            color: "#EF7930"
          }]
        }, {
          name: "集群作业信息",
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
        }],
        // 已启动变量包列表
        packageOptions: [],
        // time颗粒度列表
        granularityOptions: [],
        from: {
          packageValue: null,
          // time颗粒度
          granularity: null
        }
      };
    },
    computed: {
      jobMangerCpuEmpty() {
        if (this.jobMangerCpu.columns && this.jobMangerCpu.columns.length > 0)
          return false;
        return true;
      }
    },
    mounted() {
      getStartInfo().then(res => {
        this.packageOptions = res.rows;
        if(this.packageOptions.length > 0){
          this.from.packageValue = this.packageOptions[0].jobId;
          this.loadData();
        }
      }).catch(err => {
        console.log(err);
      })

      this.getDicts("operation_monitor_granularity").then(response => {
        this.granularityOptions = response.data;
        this.from.granularity = "10";
      });

      // 每60秒更新数据
      this.timer = setInterval(this.loadData(), 60000);
    },
    methods: {
      loadData() {
        this.chartLoading = true;
        getInfo().then(res => {
          if (res.code === 200) {
            this.cardData[0].content[0].value = res.data.coreList.total;
            this.cardData[0].content[1].value = res.data.coreList.used;
            this.cardData[0].content[2].value = res.data.coreList.available;
            this.cardData[1].content[0].value = res.data.memoryList.total;
            this.cardData[1].content[1].value = res.data.memoryList.used;
            this.cardData[1].content[2].value = res.data.memoryList.available;
            this.cardData[2].content[0].value = res.data.runingjob.runing;
            this.cardData[2].content[1].value = res.data.runingjob.stop;
          }
        }).catch(err => {
          console.log(err);
        })

        getStartInfo().then(res => {
          this.packageOptions = res.rows;
        }).catch(err => {
          console.log(err);
        })

        getChartInfo(this.from.packageValue).then(res => {
          console.log(res.data)
          this.jobMangerHead = {};
          this.jobMangerCpu = {};
          this.taskMangerCpu = {};
          this.taskMangerHead = {};
          this.avgComputerDuration = {};
          this.numRecordsOut = {};
          this.jobMangerHead['columns'] = ['time', 'jobMangerHead'];
          this.jobMangerCpu['columns'] = ['time', 'jobMangerCpu'];
          this.taskMangerCpu['columns'] = ['time', 'taskMangerCpu'];
          this.taskMangerHead['columns'] = ['time', 'taskMangerHead'];
          this.avgComputerDuration['columns'] = ['time', 'avgComputerDuration'];
          this.numRecordsOut['columns'] = ['time', 'numRecordsOut'];
          let temp_1 = [];
          let temp_2 = [];
          let temp_3 = [];
          let temp_4 = [];
          let temp_5 = [];
          let temp_6 = [];
          let data = {
            jobMangerHead: res.data.jobMangerHead ? res.data.jobMangerHead : [],
            jobMangerCpu: res.data.jobMangerCpu ? res.data.jobMangerCpu : [],
            taskMangerCpu: res.data.taskMangerCpu ? res.data.taskMangerCpu : [],
            taskMangerHead: res.data.taskMangerHead ? res.data.taskMangerHead : [],
            avgComputerDuration: res.data.avgComputerDuration ? res.data.avgComputerDuration : [],
            numRecordsOut: res.data.numRecordsOut ? res.data.numRecordsOut : []
          }
          data.jobMangerHead.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['jobMangerHead'] = item.monitorValue;
            if (temp_1.length === 0 || new Date(item.createTime).getTime() - new Date(temp_1[temp_1.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_1.push(result);
            }
          })
          data.jobMangerCpu.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['jobMangerCpu'] = item.monitorValue;
            if (temp_2.length === 0 || new Date(item.createTime).getTime() - new Date(temp_2[temp_2.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_2.push(result);
            }
          })
          data.taskMangerCpu.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['taskMangerCpu'] = item.monitorValue;
            if (temp_3.length === 0 || new Date(item.createTime).getTime() - new Date(temp_3[temp_3.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_3.push(result);
            }
          })
          data.taskMangerHead.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['taskMangerHead'] = item.monitorValue;
            if (temp_4.length === 0 || new Date(item.createTime).getTime() - new Date(temp_4[temp_4.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_4.push(result);
            }
          })
          data.avgComputerDuration.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['avgComputerDuration'] = item.monitorValue;
            if (temp_5.length === 0 || new Date(item.createTime).getTime() - new Date(temp_5[temp_5.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_5.push(result);
            }
          })
          data.numRecordsOut.forEach(item => {
            let result = {};
            result['time'] = new Date(item.createTime);
            result['numRecordsOut'] = item.monitorValue;
            if (temp_6.length === 0 || new Date(item.createTime).getTime() - new Date(temp_6[temp_6.length -
                1]['time']).getTime() >= parseInt(this.from.granularity) * 60000) {
              temp_6.push(result);
            }
          })
          this.jobMangerHead['rows'] = temp_1;
          this.jobMangerCpu['rows'] = temp_2;
          this.taskMangerCpu['rows'] = temp_3;
          this.taskMangerHead['rows'] = temp_4;
          this.avgComputerDuration['rows'] = temp_5;
          this.numRecordsOut['rows'] = temp_6;
          if(temp_1.length === 1) {
            let result = this.extend;
            result.xAxis.minInterval = 18000 * 1000;
            result.xAxis.maxInterval = 18000 * 1000;
            this.extend = Object.assign({}, this.extend, result);
          }
          else if(temp_1.length > 1){
            let num = 7200 * 1000;
            let time = new Date(temp_1[temp_1.length-1]['time']).getTime() - new Date(temp_1[0]['time']).getTime();
            if(time <= 600 * 1000) {
              num = 60 * 1000;
            }
            else if(time <= 3600 * 1000) {
              num = 600 * 1000;
            }
            else if(time <= 21600 * 1000) {
              num = 1800 * 1000;
            }
            else if(time <= 43200 * 1000) {
              num = 3600 * 1000;
            }
            let result = this.extend;
            result.xAxis.minInterval = num;
            result.xAxis.maxInterval = num;
            this.extend = Object.assign({}, this.extend, result);
          }
          this.chartLoading = false;
        }).catch(err => {
          console.log(err);
          this.chartLoading = false;
          this.chartEmpty = true;
        })
      }
    },
    beforeDestroy() {
      // 离开页面销毁定时任务
      clearInterval(this.timer);
    }
  }
</script>

<style scoped>
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

  .card-from {
    margin: 20px;
    margin-bottom: 0;
    width: 100%;
  }

  .card-from-select-box {
    align-items: center;
  }

  .card-from-select {
    width: 350px;
    display: flex;
    align-items: center;
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
