<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="68px">
      <el-form-item label="作业名称" prop="sqlTaskName">
        <el-input
          v-model="queryParams.sqlTaskName"
          placeholder="请输入作业名称"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="运行状态" prop="runStatus">
        <el-select v-model="queryParams.runStatus" placeholder="请选择运行状态" clearable size="small">
          <el-option
            v-for="dict in runStatusOptions"
            :key="dict.dictValue"
            :label="dict.dictLabel"
            :value="dict.dictValue"
          />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-plus"
          size="mini"
          @click="handleAdd"
          v-hasPermi="['system:develop:add']"
        >新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-edit"
          size="mini"
          :disabled="single"
          @click="handleUpdate"
          v-hasPermi="['system:develop:edit']"
        >修改
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDelete"
          v-hasPermi="['system:develop:remove']"
        >删除
        </el-button>
      </el-col>
      <!--      <el-col :span="1.5">-->
      <!--        <el-button type="primary" icon="el-icon-download" size="mini" @click="handleExport" v-hasPermi="['system:develop:export']">导出-->
      <!--        </el-button>-->
      <!--      </el-col>-->
    </el-row>

    <el-table v-loading="loading" :data="developList" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="55" align="center"/>
      <el-table-column label="作业名称" align="center" prop="sqlTaskName"/>
      <el-table-column label="版本号" align="center" prop="sqlTaskVersion"/>
      <el-table-column label="运行状态" align="center">
        <template slot-scope="scope">
          <el-switch
            v-model="scope.row.runStatus"
            active-value="1"
            inactive-value="0"
            @change="handleStatusChange(scope.row)"
          />
        </template>
      </el-table-column>

      <el-table-column label="启动时间" align="center" prop="startTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.startTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="停用时间" align="center" prop="stopTime" width="180">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.stopTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" align="center" class-name="small-padding fixed-width">
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            icon="el-icon-view"
            @click="handleDetail(scope.row)"
            v-hasPermi="['system:source:edit']"
          >详情
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-edit"
            @click="handleUpdate(scope.row)"
            v-hasPermi="['system:develop:edit']"
          >版本升级
          </el-button>
          <el-button
            size="mini"
            type="text"
            icon="el-icon-delete"
            @click="handleDelete(scope.row)"
            v-hasPermi="['system:develop:remove']"
          >删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <pagination
      v-show="total>0"
      :total="total"
      :page.sync="queryParams.pageNum"
      :limit.sync="queryParams.pageSize"
      @pagination="getList"
    />

    <!-- 添加或修改SQL任务开发对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="1000px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="90px">
        <el-form-item label="作业名称" prop="sqlTaskName">
          <el-input v-model="form.sqlTaskName" placeholder="请输入作业名称" style="width: 200px" :disabled="disabledInput"/>
          <span style="float: right">
              <el-button type="primary" @click="syntaxCheck" size="medium" v-show="eltabs">语法校验</el-button>
              <el-button type="primary" @click="debugSQL" size="medium" v-show="eltabs">调   试</el-button>
          </span>
        </el-form-item>
        <el-form-item label="sql编辑区" prop="sqlContent">
          <el-input v-model="form.sqlContent" type="textarea" :autosize="{ minRows: 13, maxRows: 20}"
                    :disabled="disabledSqlContent"
                    placeholder="请输入合法的sql，多个sql用分割符号 ; 隔开"/>
        </el-form-item>
        <el-tabs type="border-card" :class="{control_result_color:!sqlCheckFlag}" style="margin-left: 88px"
                 v-model="tabActiveName" v-show="eltabs">
          <el-tab-pane label="校验结果" name="first">
            <el-input v-model="sqlCheckMsg" type="textarea" :autosize="{ minRows: 7, maxRows: 10}"/>
          </el-tab-pane>
          <el-tab-pane label="输入参数" name="second">
            <div>
              <div class="el-col-18">
                <template>
                  <span style="padding: 0 10px">
                  输入表:
                    <el-select v-model="sourceTableValue" placeholder="请选择输入表" clearable>
                      <el-option v-for="item in sourceTable" :key="item.value" :label="item.label" :value="item.value"/>
                    </el-select>

                  </span>
                  <span style="padding: 0 10px">
                  输出表:
                    <el-select v-model="resultTableValue" multiple filterable allow-create default-first-option
                               placeholder="请选择输出表">
                      <el-option v-for="item in sourceResultTable" :key="item.value" :label="item.label"
                                 :value="item.value"/>
                    </el-select>
                  </span>
                </template>
              </div>
              <div>
                <el-button type="primary" @click="confirmParamTable" size="medium">确认参数表</el-button>
                <el-button type="primary" @click="runSQL" size="medium">运 行</el-button>
              </div>
            </div>

            <div>
              <template>
                <div class="boxShadow">
                  <div style="margin-top: 20px">
                    <!--动态生成表格-->
                    <el-scrollbar>
                      <el-table :data="tables" border ref="multipleTable" tooltip-effect="dark" style="width: 100%"
                                empty-text="未选择输入参数表">
                        <template v-for='(col) in tableData'>
                          <el-table-column :show-overflow-tooltip="true"
                                           :prop="col.dataItem" :label="col.dataName" :key="col.dataItem" width="200px">
                            <template scope="scope">
                              <el-input v-model="scope.row[col.dataItem]" placeholder="请输入内容"/>
                            </template>
                          </el-table-column>
                        </template>
                        <el-table-column label="操作" align="center" class-name="small-padding fixed-width"
                                         :width="tableWidth">
                          <template slot-scope="scope">
                            <el-button @click="addInput"><i class="el-icon-plus"/></el-button>
                            <el-button @click="removeInput(scope)"><i class="el-icon-minus"/></el-button>
                          </template>
                        </el-table-column>
                      </el-table>
                    </el-scrollbar>

                  </div>
                </div>
              </template>
            </div>
          </el-tab-pane>
          <el-tab-pane label="输出结果" name="third">
            <el-input v-model="sqlRunResult" type="textarea" :autosize="{ minRows: 7, maxRows: 10}"/>
          </el-tab-pane>
        </el-tabs>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="submitForm" v-show="eltabs">保 存</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
  import {
    addDevelop,
    changeTaskStatus,
    delDevelop,
    exportDevelop,
    getDevelop,
    insertDevelopHigh,
    listDevelop,
    sqlCheck,
    sqlTest
  } from "@/api/taskdevelop/sqldevelop.js";
  import {getSource, listSource} from "@/api/source/manage.js";
  import {listResult} from "@/api/source/result.js";
  import store from "../../../store";
  import {Message} from "element-ui";

  export default {
    name: "Develop",
    data() {

      // 获取数据源表表名
      const generateSourceData = _ => {
        const data = [];
        listSource(this.queryParams).then(response => {
          for (let i = 0; i < response.rows.length; i++) {
            data.push({
              value: response.rows[i].dataSourceId,
              label: `${response.rows[i].tableName}`,
              name: response.rows[i].topicName
            });
          }
        });
        return data;
      };
      // 获取数据结果表表名
      const generateResultData = _ => {
        const data = [];
        listResult(this.queryParams).then(response => {
          for (let i = 0; i < response.rows.length; i++) {
            data.push({
              value: response.rows[i].dataResultSourceId,
              label: `${response.rows[i].tableName}`
              // name: response.rows[i].dataResultSourceId
            });
          }
        });
        return data;
      };

      return {
        tables: [],
        tableData: [],
        // 是否已点击【确认参数表按钮】
        clickConfirmParamTable: false,
        // 所有的数据源表名
        sourceTable: generateSourceData(),
        //已选择的输入参数表
        sourceTableValue: '',
        // 所有的结果表名
        sourceResultTable: generateResultData(),
        //已选择的输出参数表
        resultTableValue: [],
        // 遮罩层
        loading: true,
        // 选中数组
        ids: [],
        // 非单个禁用
        single: true,
        // 非多个禁用
        multiple: true,
        // 总条数
        total: 0,
        // SQL任务开发表格数据
        developList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 运行状态字典
        runStatusOptions: [],
        // 语法校验结果信息
        sqlCheckMsg: '',
        // 调试结果
        sqlRunResult: '',
        sqlCheckFlag: false,
        // tab标签展示哪一页,进来默认显示第一页
        tabActiveName: "first",
        // 输入表的宽度
        tableWidth: "838px",
        // 所选择的资源表topic名
        sourceTopicName: "",
        nameOption: "",
        // 控制作业名称是否可修改
        disabledInput: true,
        // 控制sql编辑区是否可修改
        disabledSqlContent: false,
        // 是否暂时el-tabs
        eltabs: true,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          sqlTaskName: undefined,
          sqlContent: undefined,
          sqlTaskVersion: undefined,
          runStatus: undefined,
          startTime: undefined,
          stopTime: undefined,
          createBy: undefined,
          createTime: undefined
        },
        // 表单参数
        form: {},
        // 表单校验
        rules: {
          sqlTaskName: [
            {required: true, message: "作业名称不能为空", trigger: "blur"}
          ],
          sqlContent: [
            {required: true, message: "sql不能为空", trigger: "blur"}
          ],
          sourceTableId: [
            {required: true, message: "数据源不能为空", trigger: "blur"}
          ],
          resultTableIds: [
            {required: true, message: "数据结果表不能为空", trigger: "blur"}
          ],
          applicationId: [
            {required: true, message: "applicationId不能为空", trigger: "blur"}
          ]
        }
      };

    },
    created() {
      this.getList();
      this.getDicts("sql_task_status").then(response => {
        this.runStatusOptions = response.data;
      });
    },
    methods: {

      // 运行sql,出调试结果
      runSQL() {

        if (this.form.sqlContent === undefined) {
          this.$alert("请先填写sql！", {type: "warning"});
          return false;
        }
        if (this.clickConfirmParamTable === false) {
          this.$alert("请先确认参数表！", {type: "warning"});
          return false;
        }
        if (this.resultTableValue.length === 0) {
          this.$alert("请选择输出表！", {type: "warning"});
          return false;
        }

        // 没出结果，一直等待
        let loading = this.$loading({
          lock: true,
          text: '正在调试，请耐心等待...',
          spinner: 'el-icon-loading',
          background: 'rgba(0, 0, 0, 0.2)'
        });

        const debugParams = [];
        // 取输入表的topic名
        for (let i = 0; i < this.sourceTable.length; i++) {
          if (this.sourceTable[i].value === this.sourceTableValue) {
            this.nameOption = this.sourceTable[i].name;
            break
          }
        }
        let reg = new RegExp('\n', 'g');//g代表全部
        let tsqlStringrep = this.form.sqlContent.replace(reg, " ");

        debugParams.push({
          // sql
          sqlString: tsqlStringrep,
          // value
          values: this.tables,
          // 输入表id
          sourceTableId: this.sourceTableValue,
          // 结果表id
          resultTables: this.resultTableValue,
          // 任务名
          jobName: this.form.sqlTaskName
        });
        sqlTest(JSON.stringify(debugParams)).then(res => {
          // 关闭loading
          loading.close();
          this.tabActiveName = "third";
          // JSON.
          let reg = new RegExp('@_@', 'g');//g代表全部
          this.sqlRunResult = res.msg.replace(reg, "\n");
          let reg2 = new RegExp('},{', 'g');//g代表全部
          this.sqlRunResult = this.sqlRunResult.replace(reg2, "}\n{");
        }).catch(err => {
          loading.close();
        });
      },
      // 确认查询参数表
      confirmParamTable() {
        this.tableData = [];
        this.tables = [];
        this.tableWidth = "838PX";
        if (this.sourceTableValue === "") {
          this.$alert("请先选择输入参数表", {type: "warning"});
          return false;
        }
        this.clickConfirmParamTable = true;
        getSource(this.sourceTableValue).then(response => {
          for (let i = 0; i < response.data.dynamicItem.length; i++) {
            this.tableData.push({
              dataItem: response.data.dynamicItem[i].schemaDefine,
              dataName: response.data.dynamicItem[i].schemaDefine + "/" + response.data.dynamicItem[i].dataBaseType
            });
          }
          this.tables.push({})
          this.tableWidth = "150PX";
        });
      },

      // sql的语法校验
      syntaxCheck() {
        this.tables = [];
        this.tableData = [];
        this.tableWidth = "838PX";
        this.clickConfirmParamTable = false;
        this.tabActiveName = "first";
        if (this.form.sqlContent === undefined) {
          this.$alert("请填写需校验的sql语句", {type: "error"});
          return false;
        }
        sqlCheck(this.form.sqlContent).then(response => {
          this.sqlCheckMsg = response.msg;
          this.sqlCheckFlag = response.msg === '校验通过';
        }).catch(() => {
          }
        )
      },

      // sql调试
      debugSQL() {
        this.tabActiveName = "second";
      },

      // 新增输入行
      addInput() {
        this.tables.push({})
      },
      // 移除输入行
      removeInput(scope) {
        this.tables.splice(scope.$index, 1);
      },

      // 作业状态的启动与停用
      handleStatusChange(row) {
        let thisParam = this;
        let text = row.runStatus === "0" ? "停用" : "启用";
        this.$confirm('确认要"' + text + '""' + row.sqlTaskName + '"作业吗?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          let loading = thisParam.$loading({
            lock: true,
            text: '正在'+text+'，请耐心等待...',
            spinner: 'el-icon-loading',
            background: 'rgba(0, 0, 0, 0.2)'
          });
          changeTaskStatus(row.sqlDevelopId, row.runStatus, row.sqlTaskName, row.sourceTableId,
            row.resultTableIds, row.sqlContent, row.applicationId).then(() => {
              loading.close();
              thisParam.msgSuccess(text + "成功");
            }
          ).catch(()=>{
            loading.close();
          });
        }).catch(function () {
          row.runStatus = row.runStatus === "0" ? "1" : "0";
        });
      },

      /** 查询SQL任务开发列表 */
      getList() {
        this.loading = true;
        listDevelop(this.queryParams).then(response => {
          this.developList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 运行状态字典翻译
      runStatusFormat(row, column) {
        return this.selectDictLabel(this.runStatusOptions, row.runStatus);
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          sqlDevelopId: undefined,
          sqlTaskName: undefined,
          sqlContent: undefined,
          sqlTaskVersion: undefined,
          runStatus: "0",
          startTime: undefined,
          stopTime: undefined,
          createBy: undefined,
          createTime: undefined
        };
        this.resetForm("form");
        this.sqlCheckMsg = '';
        this.sourceTableValue = '';
        this.resultTableValue = [];
        this.tableData = [];
        this.tables = [];
        this.clickConfirmParamTable = false;
        this.sqlRunResult = '';
        this.tabActiveName = "first";
        this.tableWidth = "838PX";
        this.disabledInput = true;
        this.disabledSqlContent = false;
      },
      /** 搜索按钮操作 */
      handleQuery() {
        this.queryParams.pageNum = 1;
        this.getList();
      },
      /** 重置按钮操作 */
      resetQuery() {
        this.resetForm("queryForm");
        this.handleQuery();
      },
      // 多选框选中数据
      handleSelectionChange(selection) {
        this.ids = selection.map(item => item.sqlDevelopId);
        this.single = selection.length !== 1;
        this.multiple = !selection.length
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.eltabs = false;
        this.reset();
        this.disabledSqlContent = true;
        const sqlDevelopId = row.sqlDevelopId || this.ids;
        getDevelop(sqlDevelopId).then(response => {
          this.form = response.data;
          this.open = true;
          this.title = "查看sql作业";
        });
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.disabledSqlContent = false;
        this.eltabs = true;
        this.reset();
        this.open = true;
        this.title = "新增作业";
        this.disabledInput = false;
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.disabledSqlContent = false;
        this.eltabs = true;
        this.disabledInput = true;
        this.reset();
        const sqlDevelopId = row.sqlDevelopId || this.ids;
        getDevelop(sqlDevelopId).then(response => {
          this.form = response.data;
          this.open = true;
          this.title = "升级SQL任务开发";
        });
      },
      /** 提交按钮 */
      submitForm: function () {
        this.$refs["form"].validate(valid => {
          if (valid) {
            if (this.sourceTableValue === "") {
              this.$alert("请先选择输入参数表", {type: "warning"});
              this.tabActiveName = "second";
              return false;
            }
            if (this.resultTableValue.length === 0) {
              this.$alert("请选择输出表！", {type: "warning"});
              this.tabActiveName = "second";
              return false;
            }
            // 校验sql语法
            this.syntaxCheck();
            if (this.sqlCheckFlag) {
              if (this.form.sqlDevelopId !== undefined) {
                insertDevelopHigh(this.form).then(response => {
                  if (response.code === 200) {
                    this.msgSuccess(response.msg);
                    this.sqlCheckFlag = false;
                    this.open = false;
                    this.getList();
                  } else {
                    this.msgError(response.msg);
                  }
                });
              } else {
                // 新增版本为 1.0
                this.form.sqlTaskVersion = "1.0";
                this.form.sourceTableId = this.sourceTableValue;
                // let ids = "";
                // for (let i = 0; i < this.sourceResultTable.length; i++) {
                //   if(this.resultTableValue.includes(this.sourceResultTable[i].value)){
                //     ids += this.sourceResultTable[i].name + ",";
                //   }
                // }
                // this.form.resultTableIds = ids.substr(0,ids.length-1);
                let ids = "";
                for (let i = 0; i < this.resultTableValue.length; i++) {
                  ids += this.resultTableValue[i] + ",";
                }
                this.form.resultTableIds = ids.substr(0, ids.length - 1);
                addDevelop(this.form).then(response => {
                  if (response.code === 200) {
                    this.msgSuccess("新增成功");
                    this.sqlCheckFlag = false;
                    this.open = false;
                    this.getList();
                  } else {
                    this.msgError(response.msg);
                  }
                });
              }
            }
          }
        });
      },
      /** 删除按钮操作 */
      handleDelete(row) {
        const sqlDevelopIds = row.sqlDevelopId || this.ids;
        this.$confirm('是否确认删除该SQL任务开发数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return delDevelop(sqlDevelopIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function () {
        });
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有SQL任务开发数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return exportDevelop(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function () {
        });
      }
    }
  };
</script>

<style scoped>
  /*设置输入框的字体眼色*/
  .control_result_color >>> .el-textarea__inner {
    color: red;
  }

  /*设置滚动轴的样式*/
  .boxShadow >>> .el-scrollbar__bar {
    position: absolute;
    right: 6px;
    bottom: 10px;
    z-index: 1;
    border-radius: 4px;
    opacity: 0;
    transition: opacity 120ms ease-out;
  }

  .boxShadow >>> .el-scrollbar__bar.is-horizontal {
    height: 0;
    left: 2px;
  }
</style>
