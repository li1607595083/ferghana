<template>
  <div class="app-container">
    <div v-show="layoutOne">

      <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="120px">
        <el-form-item label="变量包中文名" prop="variablePackName">
          <el-input
            v-model="queryParams.variablePackName"
            placeholder="请输入变量包中文名"
            clearable
            size="small"
            @keyup.enter.native="handleQuery"
          />
        </el-form-item>
        <el-form-item label="运行状态" prop="runingState">
          <el-select v-model="queryParams.runingState" placeholder="请选择运行状态" clearable size="small">
            <el-option
              v-for="dict in runingStateOptions"
              :key="dict.dictValue"
              :label="dict.dictLabel"
              :value="dict.dictValue"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="变量分类" prop="variableClassification">
          <el-input
            v-model="queryParams.variableClassification"
            placeholder="请输入变量分类"
            clearable
            size="small"
            @keyup.enter.native="handleQuery"
          />
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
          >删除
          </el-button>
        </el-col>
      </el-row>

      <el-table v-loading="loading" :data="managerList" @selection-change="handleSelectionChange">
        <el-table-column type="selection" width="55" align="center"/>
        <el-table-column label="变量包中文名" align="left" header-align="center" prop="variablePackName"/>
        <el-table-column label="变量包英文名" width="200" align="left" header-align="center" prop="variablePackEn"/>
        <el-table-column label="变量分类"  width="250" align="center" prop="variableClassificationName"/>
        <el-table-column label="版本号" width="80" align="center" prop="versionNum"/>
        <el-table-column label="运行状态"  width="100" align="center" prop="runingState" :formatter="runingStateFormat">
          <template slot-scope="scope">
            <el-switch
              v-model="scope.row.runingState"
              active-value="1"
              inactive-value="0"
              @change="handleStatusChange(scope.row)"
            />
          </template>
        </el-table-column>
        <el-table-column label="创建时间" align="center" prop="createTime" width="180">
          <template slot-scope="scope">
            <span>{{ parseTime(scope.row.createTime) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="修改时间"  align="center" prop="modifyTime" width="180">
          <template slot-scope="scope">
            <span>{{ parseTime(scope.row.modifyTime) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="200" align="center" class-name="small-padding fixed-width">
          <template slot-scope="scope">
            <el-button
              size="mini"
              type="text"
              icon="el-icon-view"
              @click="versionDetail(scope.row)"
            >详情
            </el-button>
            <el-button
              size="mini"
              type="text"
              icon="el-icon-edit"
              @click="handleUpdate(scope.row)"
            >修改
            </el-button>
            <el-button
              size="mini"
              type="text"
              icon="el-icon-delete"
              @click="handleDelete(scope.row)"
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
    </div>

    <!-- 添加或修改变量包管理对话框 -->
    <div v-show="addDiv">
      <el-form ref="form" :model="form" :rules="rules" label-width="130px" class="el-col-24">
        <el-form-item label="变量包中文名" prop="variablePackName" class="el-col-12">
          <el-input v-model="form.variablePackName" placeholder="请输入变量包中文名" :disabled="UnAllowedUpdate"/>
        </el-form-item>
        <el-form-item label="变量包英文名" prop="variablePackEn" class="el-col-12">
          <el-input v-model="form.variablePackEn" placeholder="请输入变量包英文名" :disabled="UnAllowedUpdate"/>
        </el-form-item>
        <el-form-item label="版本号" class="el-col-12" v-show="versionNumShow">
          <el-input v-model="form.versionNum" :disabled="UnAllowedUpdate"/>
        </el-form-item>
        <el-form-item label="变量分类" prop="variableClassification" class="el-col-12">
          <el-select v-model="form.variableClassification" placeholder="请选择变量类型" @change="variableClassificationChange"
                     clearable style="width: 100%" :disabled="UnAllowedUpdate">
            <el-option
              v-for="data in variableClassificationOptions"
              :key="data.value"
              :label="data.name"
              :value="data.value"
              :sourceTableName="data.sourceTableName"
              :dimensionRelation="data.dimensionRelation"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="数据结果表:" prop="resultTable" class="el-col-12">
          <el-select v-model="form.resultTable" placeholder="请选择数据结果表"
                     clearable style="width: 100%" :disabled="detailViem">
            <el-option
              v-for="data in resultTableOptions"
              :key="data.value"
              :label="data.name"
              :value="data.value"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="加工变量" class="el-col-24" prop="variableId">
          <treeselect v-model="form.variableId" :options="variableCenter" :multiple="true" id="variableId"
                      @select="variableCenterSelect" @deselect="variableCenterDeSelect" :disableBranchNodes="true"
                      :showCount="true"
                      :clearable="false"
                      placeholder="请选择加工变量" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="原始变量" class="el-col-24" >
          <el-select v-model="form.originalVariable" placeholder="请选择原始变量" multiple clearable no-data-text="未选择变量分类"
                     style="width: 100%" :disabled="detailViem">
            <el-option-group :label="dataSourceName">
              <el-option
                v-for="data in dataSourcFields"
                :key="data.value"
                :label="data.label"
                :value="data.value"
                :type="data.type"
              />
            </el-option-group>
            <div v-for="dataAll in listDimension">
              <el-option-group :label="dataAll.name" size="medium">
                <el-option
                  v-for="data in dataAll.dimensionRelationOptions"
                  :key="data.value"
                  :label="data.label"
                  :value="data.value"
                />
              </el-option-group>
            </div>
          </el-select>
        </el-form-item>
        <el-form-item label="备注" prop="description" class="el-col-24">
          <el-input v-model="form.description" type="textarea" placeholder="请输入备注" :disabled="detailViem"/>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer" style="float: right">
        <el-button type="primary" @click="testRun" :disabled="detailViem">测 试</el-button>
        <el-button type="primary" @click="submitForm" :disabled="detailViem">确 定</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </div>

    <!--        测试模块-->
    <div>
      <template>

        <div class="boxShadow">
          <el-dialog :title="title" :visible.sync="open" customClass="customWidth" :close-on-click-modal="false"
                     class="el-col-24" :show-close="false">
            <el-tabs v-model="activeName" type="card">

              <el-tab-pane label="测试数据" name="first">
                <div v-show="sourceTableValueItem">
                  <el-scrollbar>
                    <span style="font-size: 16px;font-weight: bold">数据源表字段</span>
                    <el-table :data="form.sourceTableValue" border ref="multipleTable" tooltip-effect="dark"
                              style="width: 100%; margin-top: 10px"
                              empty-text="未选择输入参数表">
                      <template v-for='(col,index) in sourceTableCol'>
                        <el-table-column :show-overflow-tooltip="true"
                                         :prop="col.dataItem" align="center" :label="col.dataName" :key="index"
                                         width="300px">
                          <template scope="scope">
                            <el-input v-model="scope.row[col.dataItem]" placeholder="请输入内容"/>
                          </template>
                        </el-table-column>
                      </template>
                      <el-table-column label="操作" width="300px" align="center"
                                       class-name="small-padding fixed-width">
                        <template slot-scope="scope">
                          <el-button @click="addInputSource"><i class="el-icon-plus"/></el-button>
                          <el-button @click="removeInputSource(scope)"><i class="el-icon-minus"/></el-button>
                        </template>
                      </el-table-column>
                    </el-table>
                  </el-scrollbar>
                </div>
                <div v-show="dimensionTableValueItem">
                  <el-scrollbar>
                    <div v-for="dataAll in listResultDimension" style="margin-top: 10px">
                      <span style="font-size: 16px;font-weight: bold">{{dataAll.name}}</span>
                      <el-table :data="dataAll.dimensionTableValue" border ref="multipleTable" tooltip-effect="dark"
                                style="width: 100%; margin-top: 10px"
                                empty-text="未选择输入参数表">
                        <template v-for='(col,index) in dataAll.col'>
                          <el-table-column :show-overflow-tooltip="true"
                                           :prop="col.dataItem" align="center" :label="col.dataName" :key="index"
                                           width="300px">
                            <template scope="scope">
                              <el-input v-model="scope.row[col.dataItem]" placeholder="请输入内容"/>
                            </template>
                          </el-table-column>
                        </template>
                        <el-table-column label="操作" width="300px" align="center" class-name="small-padding fixed-width">
                          <template slot-scope="scope">
                            <el-button @click="addInputDimension(dataAll.name)"><i class="el-icon-plus"/></el-button>
                            <el-button @click="removeInputDimension(scope,dataAll.name)"><i class="el-icon-minus"/>
                            </el-button>
                          </template>
                        </el-table-column>
                      </el-table>
                    </div>
                  </el-scrollbar>
                </div>
              </el-tab-pane>
              <el-tab-pane label="测试结果" name="second">
                <div class="table">
                  <el-table
                    :data="testResultData"
                    tooltip-effect="dark"
                    border
                    stripe
                    style="width: 100%">
                    <template v-for='(col,index) in testResultCol'>
                      <el-table-column :show-overflow-tooltip="true" align="center" :prop="col.dataItem"
                                       :label="col.dataName" :key="index"
                                       width="200px">
                      </el-table-column>
                    </template>
                  </el-table>
                </div>
              </el-tab-pane>
            </el-tabs>
            <div style="margin-left: 790px;margin-top:10px">
              <el-button type="primary" @click="confirmTest">开 始</el-button>
              <el-button @click="cancelTest">关 闭</el-button>
            </div>

          </el-dialog>
        </div>
      </template>
    </div>

    <!--  资源配置模板  -->
    <el-dialog title="资源配置" :visible.sync="resourceOpen" width="550px" :close-on-click-modal="false"
               :show-close="false">
      <el-form ref="resourceForm" :model="resourceForm" :rules="rules" class="el-col-24">
        <div class="el-col-24">
          <el-form-item class="el-col-24" prop="radio" label="配置类型：">
            <el-radio v-model="resourceForm.radio" label="radio1" @change="radioChange">智能配置</el-radio>
            <el-radio v-model="resourceForm.radio" label="radio2" @change="radioChange">手动配置</el-radio>
            <el-radio v-model="resourceForm.radio" label="radio3" :disabled="radio3Disable" @change="radioChange">使用上次资源配置</el-radio>
          </el-form-item>
        </div>
        <div v-show="resourceDiv" class="el-col-24">
          <el-form-item label="并发数:" prop="concurrency" label-width="160px">
            <el-input-number v-model="resourceForm.concurrency" :min=1 :max="10000000" :step="1" />
          </el-form-item>
          <el-form-item label="jobManager堆内存:" prop="jobMemory" label-width="160px">
            <el-input-number v-model="resourceForm.jobMemory" :min=1024 :max="10000000" :step="1024" />
            MB
          </el-form-item>
          <el-form-item label="taskManager堆内存:" prop="taskMemory" label-width="160px">
            <el-input-number v-model="resourceForm.taskMemory" :min=1024 :max="10000000" :step="1024" />
            MB
          </el-form-item>
        </div>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="resourceSubmit">启 动</el-button>
        <el-button @click="resourceCancel">取 消</el-button>
      </div>
    </el-dialog>

    <!--    版本详情-->
    <div v-show="detailDiv">
      <el-tabs v-model="versionTabName" type="card">
        <el-tab-pane label="版本历史" name="versionFirst">
          <el-table v-loading="loading" :data="versionList">
            <el-table-column label="序号" width="55" align="center" type="index"/>
            <el-table-column label="变量中文名" align="center" prop="variablePackName"/>
            <el-table-column label="版本号" align="center" prop="versionNum"/>
            <el-table-column label="版本制定者"  align="center" prop="createBy"/>
            <el-table-column label="创建时间" align="center" prop="createTime" >
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.createTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="修改时间" align="center" prop="modifyTime" width="180">
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.modifyTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="操作" align="center" class-name="small-padding fixed-width">
              <template slot-scope="scope">
                <el-button
                  size="mini"
                  type="text"
                  icon="el-icon-view"
                  @click="handleDetail(scope.row)"
                >详情
                </el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
        <el-tab-pane label="操作历史" name="versionSecond">
          <el-table v-loading="loading" :data="operateList">
            <el-table-column label="序号" width="55" align="center" type="index"/>
            <el-table-column label="操作人" align="center" prop="operateAuther"/>
            <el-table-column label="操作类型" align="center" prop="operateType"/>
            <el-table-column label="ip地址"  align="center" prop="operateIp"/>
            <el-table-column label="操作时间" align="center" prop="createTime" >
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.createTime) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
      <div style="float: right" >
        <el-button  type="primary" @click="cancel" style="margin-right: 100px">返  回</el-button>
      </div>
    </div>

  </div>
</template>

<script>
  import {
    listManager,
    getManager,
    listVersionManager,
    delManager,
    addManager,
    updateManager,
    exportManager,
    startVariablePackage,
    stopPackRun,
    testPackage
  } from "@/api/variable/package_manager";
  import {listPackageOperate} from "@/api/variable/variablePackageOperate";
  import axios from "axios";
  import {getToken} from '@/utils/auth';
  import Treeselect from '@riophae/vue-treeselect';
  import '@riophae/vue-treeselect/dist/vue-treeselect.css';
  import{isLegitimateName} from "@/utils/validate.js";

  export default {
    name: "packageManager",
    components: {Treeselect},
    data() {
      return {
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
        // 变量包管理表格数据
        managerList: [],
        versionList: [],
        operateList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 资源配置弹出层
        resourceOpen: false,
        // 运行状态字典
        runingStateOptions: [],
        // 变量因子options
        variableClassificationOptions: [],
        // 数据结果表options
        resultTableOptions: [],
        // 穿梭框标题
        transferTitles: ['变量中心', '已选择变量 '],
        // 穿梭框的搜索框
        filterableFlag: true,
        // 变量中心值
        variableCenter: [],
        //
        layoutOne: true,
        // 新增展示参数
        addDiv: false,
        // 控制手动配置资源div
        resourceDiv: false,
        versionNumShow: false,
        //
        packageRow: undefined,

        //tabs标签页的初始页
        activeName: 'first',
        // 控制数据维表列是否展示
        dimensionTableValueItem: false,
        sourceTableValueItem: false,
        // 数据源表头列
        sourceTableCol: [],
        // 数据维表头列
        dimensionTableCol: [],
        // 测试的结果列
        testResultCol: [],
        // 测试的结果
        testResultData: [],
        detailViem: false,
        detailDiv: false,
        // 控制选项修改时是否可修改
        UnAllowedUpdate: false,
        versionTabName: 'versionFirst',
        // 数据源表的测试字段
        listResultDimension: [],
        // 上次资源配置
        radio3Disable: false,
        concurrencyConfigure : "",
        taskMemoryConfigure : "",
        jobMemoryConfigure : "",

        // 资源配置单选框
        resourceForm: {
          radio: "",
          concurrency: "",
          jobMemory: "",
          taskMemory: "",
        },
        options: [],

        // selectVariableNote
        selectVariableNote: [],
        // 数据源表表名
        dataSourceName:undefined,
        // 数据源表字段
        dataSourcFields:[],
        // 数据维表
        listDimension: [],
        // variableNum 该变量包的变量个数
        variableNum:0,

        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          variablePackName: undefined,
          runingState: undefined,
          variablePackEn: undefined,
          variableClassification: undefined,
        },
        // 表单参数
        form: {
          sourceTableValue: []
        },
        // 表单校验
        rules: {
          variablePackName: [
            {required: true, message: "变量包中文名不能为空", trigger: "blur"}
          ],
          variablePackEn: [
            {required: true, message: "变量包英文名不能为空", trigger: "blur"},
            {validator: isLegitimateName, message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母", trigger: "blur"},
          ],
          variableClassification: [
            {required: true, message: "变量分类不能为空", trigger: "blur"}
          ],
          radio: [
            {required: true, message: "配置信息不能为空", trigger: "blur"}
          ],
          concurrency: [
            {required: true, message: "并发数不能为空", trigger: "blur"}
          ],
          jobMemory: [
            {required: true, message: "jobManager堆内存不能为空", trigger: "blur"}
          ],
          taskMemory: [
            {required: true, message: "taskManager堆内存不能为空", trigger: "blur"}
          ],
          variableId: [
            {required: true, message: "变量不能为空", trigger: "blur"}
          ],
          resultTable: [
            {required: true, message: "结果表不能为空", trigger: "blur"}
          ],

        }
      };
    },
    created() {
      this.getList();
      this.getDicts("sql_task_status").then(response => {
        this.runingStateOptions = response.data;
      });
    },
    methods: {

      getVersionList(data) {
        this.loading = true;
        this.queryParams.variablePackEn = data.variablePackEn;
        listVersionManager(this.queryParams).then(response => {
          this.versionList = response.rows;
          this.loading = false;
          this.queryParams.variablePackEn = undefined;
        });
      },
      getOperateList(data) {
        this.loading = true;
        listPackageOperate(data.variablePackEn).then(response => {
          this.operateList = response.rows;
          this.loading = false;
          this.queryParams.variablePackEn = undefined;
        });
      },

      // 确认测试
      confirmTest() {
        console.log("++++++++qqq");
        console.log(this.form.variableId);
        let tmp = this.form.variableId;
        this.form.testResultItem = [];
        let dmo = [];
        this.form.testResultItem = this.form.variableId;
        for (let i = 0; i < this.selectVariableNote.length; i++) {
          if (this.selectVariableNote[i].variableType === '02') { // 派生变量 去除对应的基础变量 否则会在测试结果中展示出来
            let deriveBaseVariable = this.selectVariableNote[i].deriveBaseVariable;
            let parse = JSON.parse(deriveBaseVariable);
            for (let j = 0; j < parse.length; j++) {
              this.form.testResultItem.forEach((value, index) => {
                if (value === parse[j]) {
                  this.form.testResultItem.splice(index, 1);
                  dmo = dmo.concat(value);
                }
              })
            }
          }
        }

        // 添加测试的结果字段 就是基础变量的字段  。如包含派生变量，只展示派生变量，其包含的基础变量就不展示
        // 测试的结果列赋值 testResultCol 主键 + 英文名
        this.testResultCol = [];
        for (let i = 0; i < this.sourceTableCol.length; i++) {
          if (this.sourceTableCol[i].dataName.indexOf("主键") > 0) {
            this.testResultCol.push({
              dataName: this.sourceTableCol[i].dataName.substr(0, this.sourceTableCol[i].dataName.length - 3),
              dataItem: this.sourceTableCol[i].dataItem.substr(0, this.sourceTableCol[i].dataName.length - 3),
            })
            break;
          }
        }
        console.log("---1----2222--");
        console.log(this.form.testResultItem);
        for (let i = 0; i < this.form.testResultItem.length; i++) {
          this.testResultCol.push({
            dataName: this.form.testResultItem[i],
            dataItem: this.form.testResultItem[i],
          })
        }
        console.log("---1----11--");
        console.log(this.testResultCol);

        // this.form.variableId = JSON.stringify(this.form.variableId);
        this.form.testDimdata = this.listResultDimension;
        // 没出结果，一直等待
        let loading = this.$loading({
          lock: true,
          text: '正在调试，请耐心等待...',
          spinner: 'el-icon-loading',
          background: 'rgba(0, 0, 0, 0.2)'
        });
        // 恢复变量
        if (dmo.length > 0) {
          tmp = tmp.concat(dmo);
        }
        this.form.variableId = tmp;
        console.log("++++++++++++++++++++++++++++++++++++q");
        console.log(this.form.variableId);
        testPackage(this.form).then(resp => {
            loading.close();


            // this.testResultData 为测试结果数据 第一列为主键  第二列为结果
            if (resp.msg === "no success") {
              this.$alert("测试报错了！", {type: 'warning'});
            } else {
              this.testResultData = JSON.parse(resp.msg);
              this.$nextTick(() => {
                this.activeName = 'second';
              })
            }
          }
        ).catch(() => {
          loading.close();
        });

      },

      // 点击测试
      testRun() {
        this.$refs["form"].validate(valid => {
            if (valid) {
              this.open = true;
              this.sourceTableValueItem = true;
              this.title = "变量包测试";
              console.log("--q--");
              console.log(this.selectVariableNote);
              for (let i = 0; i < this.selectVariableNote.length; i++) { // 已经选中的变量

                console.log("-------2www-------------");
                console.log(this.selectVariableNote);
                let testSourceTableCol = this.selectVariableNote[i].testSourceTableCol;
                if (testSourceTableCol !== null) {
                  let parse = JSON.parse(testSourceTableCol);
                  for (let j = 0; j < parse.length; j++) { // 测试的数据结果
                    let flag = true;
                    for (let k = 0; k < this.sourceTableCol.length; k++) { // 变量的测试字段
                      if (parse[j] === this.sourceTableCol[k].dataName) {
                        flag = false;
                      }
                    }
                    if (flag === true) {
                      if (parse[j].indexOf("主键") > 0 || parse[j].indexOf("水印") > 0) {
                        this.sourceTableCol.push({
                          dataItem: parse[j].substr(0, parse[j].length - 3),
                          dataName: parse[j],
                        })
                      } else {
                        this.sourceTableCol.push({
                          dataItem: parse[j],
                          dataName: parse[j],
                        })
                      }
                    }
                  }
                }
                // listResultDimension dimensionTableCol
                let testDimensionTableCol = this.selectVariableNote[i].testDimensionTableCol;
                let parse1 = JSON.parse(testDimensionTableCol);
                for (let j = 0; j < parse1.length; j++) { // 循环第一个变量的维表字段
                  second:for (let l = 0; l < parse1[j].col.length; l++) { // 循环维表里的字段 id phone name
                    let flag1 = true; //是否新增维表
                    let flag2 = true; //是否新增字段
                    let tmp = "";
                    first:for (let k = 0; k < this.listResultDimension.length; k++) { //
                      if (this.listResultDimension[k].name === parse1[j].name) { // 同一维表 ，不要新增维表了
                        flag1 = false;
                        for (let m = 0; m < this.listResultDimension[k].col.length; m++) { // 循环已经添加了的字段 id id1
                          if (parse1[j].col[l] === this.listResultDimension[k].col[m].dataItem) { // 字段去重
                            flag2 = false;
                            break first;
                          }
                        }
                        tmp = k;
                        break first;
                      }
                    }
                    if (flag1 === true) { // 新增一个维表
                      flag2 = false;
                      this.listResultDimension.push({
                        name: parse1[j].name,
                      })
                      let dmo = [];
                      for (let k = 0; k < parse1[j].col.length; k++) {
                        dmo.push({
                          dataItem: parse1[j].col[k],
                          dataName: parse1[j].col[k],
                        });
                      }
                      let dmo2 = [];
                      dmo2.push({});
                      Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {col: dmo});
                      Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo2}); // 增加输入框
                      break second;
                    }
                    if (flag2 === true) { // 新增一个字段
                      this.listResultDimension[tmp].col.push({
                        dataItem: parse1[j].col[l],
                        dataName: parse1[j].col[l],
                      })
                    }
                  }
                }
              }

              // 若测试的数据源表有字段，展示
              if (this.sourceTableCol.length > 0) {
                this.sourceTableValueItem = true;
                this.form.sourceTableValue.push({})
              }
              // 若测试的数据维表有字段，展示
              this.dimensionTableValueItem = true;
              // this.listResultDimension.dimensionTableValue.push({})

              // 测试的结果列赋值 testResultCol 主键 + 英文名
              for (let i = 0; i < this.sourceTableCol.length; i++) {
                if (this.sourceTableCol[i].dataName.indexOf("主键") > 0) {
                  this.form.sourceKey = this.sourceTableCol[i].dataName.substr(0, this.sourceTableCol[i].dataName.length - 3)
                  break;
                }
              }
            }
          }
        )
        ;
      },

      // 取消测试
      cancelTest() {
        this.open = false;
        this.sourceTableCol = [];
        this.dimensionTableCol = [];
        this.form.sourceTableValue = [];
        this.dimensionTableValueItem = false;
        this.sourceTableValueItem = false;
        this.activeName = "first";
        this.testResultData = [];
        this.listResultDimension = [];
        // this.selfQueryTestItem = [];
      }
      ,

      // 新增输入行
      addInputSource() {
        this.form.sourceTableValue.push({})
      }
      ,
      // 移除输入行
      removeInputSource(scope) {
        if (this.form.sourceTableValue.length === 1) {
          this.$alert("不能全部删除！", {type: 'warning'});
          return;
        }
        this.form.sourceTableValue.splice(scope.$index, 1);
      }
      ,

      // 新增输入行
      addInputDimension(data) {
        for (let i = 0; i < this.listResultDimension.length; i++) {
          if (this.listResultDimension[i].name === data) {
            this.listResultDimension[i].dimensionTableValue.push({})
          }
        }
      }
      ,
      // 移除输入行
      removeInputDimension(scope, data) {
        for (let i = 0; i < this.listResultDimension.length; i++) {
          if (this.listResultDimension[i].name === data) {
            if (this.listResultDimension[i].dimensionTableValue.length === 1) {
              this.$alert("不能全部删除！", {type: 'warning'});
              return;
            }
            this.listResultDimension[i].dimensionTableValue.splice(scope.$index, 1);
          }
        }
      },

      // 删除变量时
      variableCenterDeSelect(node, instanceId) {
        // 控制已选择的变量
        for (let i = 0; i < this.selectVariableNote.length; i++) {
          if (this.selectVariableNote[i].id === node.id) {
            this.selectVariableNote.splice(i, 1);
          }
        }
        let variableType = node.variableType;
        if ('02' === variableType) { // 派生变量 取消需同时取消
          let deriveBaseVariable = node.deriveBaseVariable;
          let parse = JSON.parse(deriveBaseVariable);
          this.$nextTick(function () {
            for (let i = 0; i < parse.length; i++) {
              this.form.variableId.splice(this.form.variableId.indexOf(parse[i]), 1);
            }

          })
        } else if ('01' === variableType) { // 基础变量  需检查是否为派生变量的基础变量，若是则不允许删除
          // 先遍历已选择的变量，选出派生变量，获取该派生变量的基础变量，判断是否在其中
          if (this.variableCenter[1].children != null && this.variableCenter[1].children.length > 0) {
            for (let j = 0; j < this.variableCenter[1].children.length; j++) {
              let variable = this.variableCenter[1].children[j].deriveBaseVariable;
              let id = this.variableCenter[1].children[j].id;
              let label = this.variableCenter[1].children[j].label;
              // 若派生变量被选中  并且  派生变量的基础变量为即将取消的这个基础变量
              if (this.form.variableId.indexOf(id) > 0 && variable.indexOf(node.id) > 0) {
                this.$nextTick(function () {
                  this.form.variableId = this.form.variableId.concat(node.id);
                })
                this.$alert("该基础变量被【" + label + "】派生变量所引用，请先删除【" + label + "】", {type: "error"});
              }
            }
          }
        }
      }
      ,
      // 选中变量时
      variableCenterSelect(node, instanceId) {
        // 控制已选择的变量
        this.selectVariableNote = this.selectVariableNote.concat(node);
        let id = node.id;
        let variableType = node.variableType;
        if ('02' === variableType) { // 派生变量
          let deriveBaseVariable = node.deriveBaseVariable;
          let parse = JSON.parse(deriveBaseVariable);
          this.$nextTick(function () {
            for (let i = 0; i < parse.length; i++) {
              // 新增派生变量时，需检查其关联的基础变量是否已经有选中，若已选中了的，不要重复增加
              if (this.form.variableId.indexOf(parse[i]) < 0) {
                this.form.variableId = this.form.variableId.concat(parse[i]);
              }
            }
          });
        }
        this.$forceUpdate();
      },
      // 切换变量分类
      sourceDataChange(value) {
        console.log("--1");
        console.log(value);
        setTimeout(_ => {
          let that = this;
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/source/manage/querySchema',
            headers: {'Authorization': 'Bearer ' + getToken()},
            responseType: 'json',
            params: {
              dataSourceName: value
            }
          }).then(function (resp) {
            console.log("--");
            console.log(resp);
            that.dataSourceName = "数据源表："+value;
            that.dataSourcFields = [];
            for (let j = 0; j < resp.data.rows[0].length; j++) {
              that.dataSourcFields.push({
                label: value + "." + resp.data.rows[0][j].key, // 表名.字段
                value: value + "." + resp.data.rows[0][j].key,
                type: resp.data.rows[0][j].value,// 类型
              });
            }

          }).catch(resp => {
            console.log('获取数据源表请求失败!' + resp);
          });

        }, 300);
      },

      // 查询所有的变量
      getVariableCenter(value) {
        let that = this;
        that.variableCenter = [];
        if (value !== "") {
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/variable/manager/listGroup',
            headers: {'Authorization': 'Bearer ' + getToken()},
            responseType: 'json',
            params: {
              variableClassification: value,
            }
          }).then(function (resp) {
            that.variableCenter.push(
              {
                id: 'A',
                label: '基础变量',
                children: [{
                  id: 'A1',
                  label: '普通查询',
                  children: [],
                }, {
                  id: 'A2',
                  label: '统计查询',
                  children: [],
                }, {
                  id: 'A3',
                  label: '数据加工',
                  children: [],
                }, {
                  id: 'A4',
                  label: '自定义查询',
                  children: [],
                }],
              }, {
                id: 'B',
                label: '派生变量',
                children: [],
              }
            );
            for (let i = 0; i < resp.data.rows.length; i++) {
              if (resp.data.rows[i].variableType === '01') { // 基础变量
                if (resp.data.rows[i].variableModelType === '01') {
                  that.variableCenter[0].children[0].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '02') {
                  that.variableCenter[0].children[1].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '03') {
                  that.variableCenter[0].children[2].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '04') {
                  that.variableCenter[0].children[3].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                }
              } else if (resp.data.rows[i].variableType === '02') { // 基础变量
                that.variableCenter[1].children.push(
                  {
                    id: resp.data.rows[i].variableNameEn,
                    label: resp.data.rows[i].variableName,
                    variableType: resp.data.rows[i].variableType,
                    deriveBaseVariable: resp.data.rows[i].deriveBaseVariable,
                    testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                    testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                  }
                );
              }
            }
          }).catch(resp => {
            console.log('获取变量分类请求失败：' + resp.status + ',' + resp.statusText);
          });
        }
      }
      ,
      // 启动取消按钮
      resourceCancel() {

        this.resourceOpen = false;
        this.resourceReset();
        this.packageRow.runingState = '0'; // 取消按钮不变

      }
      ,
      // 资源控制form清除
      resourceReset() {
        this.resourceDiv = false;
        this.resourceForm = {
          radio: "",
          concurrency: "",
          jobMemory: "",
          taskMemory: "",
        }
      }
      ,
      // 启动确定按钮
      resourceSubmit() {
        this.$refs["resourceForm"].validate(valid => {
          if (valid) {
            this.packageRow.configurationType = this.resourceForm.radio;
            if(this.resourceForm.radio === "radio2"){
              this.packageRow.concurrency = this.resourceForm.concurrency;
              this.packageRow.jobMemory = this.resourceForm.jobMemory;
              this.packageRow.taskMemory = this.resourceForm.taskMemory;
            } else if(this.resourceForm.radio === "radio3"){
              if (this.packageRow.concurrency === null){
                this.$alert("该变量包还未有上次配置！", {type: "warning"});
                return;
              }
            }
            let loading = this.$loading({
              lock: true,
              text: '正在启动，请耐心等待...',
              spinner: 'el-icon-loading',
              background: 'rgba(0, 0, 0, 0.2)'
            });
            startVariablePackage(this.packageRow).then(response => {
              loading.close();
              if (response.msg === "success") {
                this.msgSuccess("启动成功");
                this.getList();
              } else if (response.msg === "failure") {
                this.msgError("启动失败");
                this.packageRow.runingState = '0'; // 取消按钮不变
              }
              this.resourceOpen = false;
            }).catch(() => {
              loading.close();
              this.msgError("启动失败");
              this.packageRow.runingState = '0'; // 取消按钮不变
            })
          }
        })
      }
      ,

      // 资源配置
      radioChange(value) {
        this.resourceDiv = true;
        if (value === "radio1") {
          let num = Math.ceil(this.variableNum/5);
          this.resourceForm.concurrency = num;
          this.resourceForm.jobMemory = num * 1024;
          this.resourceForm.taskMemory = num * 1024;
        } else if (value === "radio2") {
          this.resourceForm.concurrency = 1;
          this.resourceForm.jobMemory = 1024;
          this.resourceForm.taskMemory = 1024;
        } else if (value === "radio3") {
          this.resourceForm.concurrency = this.concurrencyConfigure;
          this.resourceForm.jobMemory = this.taskMemoryConfigure;
          this.resourceForm.taskMemory = this.jobMemoryConfigure;
        }
      }
      ,
      // 作业状态的启动与停用
      handleStatusChange(row) {
        let that = this;
        if (row.runingState === '1') { // 启动
          if (!Array.isArray(row.variableId)){
            row.variableId = JSON.parse(row.variableId);
          }
          that.packageRow = row;
          that.resourceOpen = true;

          // 判断该变量所包含的变量个数，用于智能配置资源
          this.variableNum = row.variableId.length;

          this.concurrencyConfigure = row.concurrency;
          this.taskMemoryConfigure = row.taskMemory;
          this.jobMemoryConfigure = row.jobMemory;

          this.radio3Disable = row.concurrency === null || row.concurrency === "" ;

        } else if (row.runingState === '0') {  // 取消
          let text = row.runingState === "0" ? "停用" : "启用";
          that.$confirm('确认要' + text + '"' + row.variablePackName + '"变量包吗?', "警告", {
            confirmButtonText: "确定",
            cancelButtonText: "取消",
            type: "warning"
          }).then(function () {
            let loading = that.$loading({
              lock: true,
              text: '正在停用，请耐心等待...',
              spinner: 'el-icon-loading',
              background: 'rgba(0, 0, 0, 0.2)'
            });
            stopPackRun(row).then(function (res) {
              loading.close();
              if (res.msg === "success") {
                that.$alert("停用成功！", {type: 'warning'});
                that.getList();
              } else if (res.msg === "failure") {
                that.$alert("停用失败！", {type: 'warning'});
                that.packageRow.runingState = '1'; // 取消按钮不变
              }
            }).catch(function () {
              row.runingState = '1';
              loading.close();
            });
          }).catch(function () {
            row.runingState = '1';
          });

        }

      },
      getResultTable() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/source/result/list',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json'
        }).then(function (resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            that.resultTableOptions.push({
              value: resp.data.rows[i].tableName,
              name: resp.data.rows[i].tableName,
            });
          }
        }).catch(resp => {
          console.log('获取数据源结果表请求失败!');
        });
      },
      // 数据维表表字段查询
      getDimensionRelationFild(value) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        console.log("--11--1--1-1--");
        console.log(value);
        let parse = JSON.parse(value);
        let param = "";
        for (let i = 0; i < parse.length; i++) {
          param = param + parse[i].dimensionName + ","
        }
        axios({
          method: 'get',
          url: baseUrl + '/system/Dimension/querySchema',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            dimensionNames: param.substr(0, param.length - 1)
          }
        }).then(function (resp) {
          console.log("-222--");
          console.log(param);
          console.log(resp);
          that.listDimension = [];
          // 拼接变量因子的options
          for (let i = 0; i < resp.data.rows.length; i++) {
            for (let key in resp.data.rows[i]) {
              let strings = key.split("&&");
              that.listDimension.push({
                name: '数据维表:' + strings[0],  // 表名
                primaryKeyLabel: strings[1],    // 主键 格式：  xxx-主键
                primaryKeyValue: strings[2],    // 主键值 格式： 表名.主键
              });
              let s = [];
              for (let j = 0; j < resp.data.rows[i][key].length; j++) {
                s.push({
                  value: strings[0]+"."+resp.data.rows[i][key][j].key,
                  label: strings[0]+"."+resp.data.rows[i][key][j].key,
                  type: resp.data.rows[i][key][j].value,
                })
              }
              // 变量因子等等的字段
              Object.assign(that.listDimension[i], {dimensionRelationOptions: s});
              // 自定义的选项字段
              Object.assign(that.listDimension[i], {defineDimensionFieldItem: s});

              let s2 = [];
              for (let j = 0; j < resp.data.rows[i][key].length; j++) {
                s2.push({
                  value: strings[0]+"."+resp.data.rows[i][key][j].key,
                  label: strings[0]+"."+resp.data.rows[i][key][j].key,
                  type: resp.data.rows[i][key][j].value,
                })
              }
              // 条件选项的字段
              Object.assign(that.listDimension[i], {dimensionStatisticsfieldOptions: s2});
            }
          }
        }).catch(resp => {
          console.log('获取数据源表id' + val[0] + '失败!' + resp);
        });
      },
      //查询变量分类的值
      getVariableClassificationOptions() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/classification/list',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json'
        }).then(function (resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            that.variableClassificationOptions.push({
              value: resp.data.rows[i].variableClassificationId,
              name: resp.data.rows[i].variableClassificationName,
              sourceTableName: resp.data.rows[i].sourceDabRelation,
              dimensionRelation: resp.data.rows[i].dimensionRelation,
            });
          }
        }).catch(resp => {
          console.log('获取变量分类请求失败：' + resp.status + ',' + resp.statusText);
        });
      }
      ,

      // 变量分类的切换
      variableClassificationChange(value) {
        // 变化对应的变量中心
        this.getVariableCenter(value);
        console.log("--2");
        console.log(value);
        console.log(this.variableClassificationOptions);
        this.variableClassificationOptions.map((data, i) => {
          if (data.value === value) {
            this.sourceDataChange(data.sourceTableName);
            this.getDimensionRelationFild(data.dimensionRelation);
            // 数据源表赋值
            this.form.sourceTableName = data.sourceTableName;
            this.form.dimensionRelation = data.dimensionRelation;
          }
        });
      }
      ,

      /** 查询变量包管理列表 */
      getList() {
        this.loading = true;
        listManager(this.queryParams).then(response => {
          this.managerList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 运行状态字典翻译
      runingStateFormat(row, column) {
        return this.selectDictLabel(this.runingStateOptions, row.runingState);
      }
      ,
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
        this.layoutOne = true;
        this.versionNumShow = false;
        this.addDiv = false;
        this.detailDiv = false;
      }
      ,
      // 表单重置
      reset() {
        this.form = {
          variablePackId: undefined,
          variablePackName: undefined,
          variablePackEn: undefined,
          runingState: undefined,
          variableClassification: undefined,
          description: undefined,
          variableId: [],
          createTableSql: undefined,
          runingSql: undefined,
          createTime: undefined,
          modifyTime: undefined,
          dimensionRelation: undefined,
          resultTable: undefined,
          selectVariableNoteForm: undefined,
          // 数据源表输入字段
          sourceTableValue: [],
          // 主键
          sourceKey: undefined,
          testDimdata: undefined,
          // 原始变量
          originalVariable: undefined,
          //
          testResultItem: [],
        };
        this.resetForm("form");
        this.variableCenter = [];
        this.resultTableOptions = [];
        this.variableClassificationOptions = [];
        this.variableNum = 0;

      }
      ,
      /** 搜索按钮操作 */
      handleQuery() {
        this.queryParams.pageNum = 1;
        this.getList();
      }
      ,
      /** 重置按钮操作 */
      resetQuery() {
        this.resetForm("queryForm");
        this.handleQuery();
      }
      ,
      // 多选框选中数据
      handleSelectionChange(selection) {
        this.ids = selection.map(item => item.variablePackId);
        this.single = selection.length !== 1;
        this.multiple = !selection.length
      }
      ,
      /** 新增按钮操作 */
      handleAdd() {
        this.detailViem = false;
        this.reset();
        this.title = "添加变量包管理";
        this.layoutOne = false;
        this.versionNumShow = false;
        this.UnAllowedUpdate = false;
        this.addDiv = true;

        // 查询变量分类
        this.getVariableClassificationOptions();
        // 查询数据结果表
        this.getResultTable();

      },

      // 查看变量的版本详细
      versionDetail(row){
        this.detailDiv = true;
        this.layoutOne = false;
        this.versionNumShow = true;
        this.UnAllowedUpdate = true;
        this.getVersionList(row);
        this.getOperateList(row);
      },


      /** 详情按钮操作 */
      handleDetail(row) {
        this.detailViem = true;
        this.reset();
        this.title = "查看变量包管理";
        this.layoutOne = false;
        this.versionNumShow = true;
        this.UnAllowedUpdate = true;
        this.detailDiv = false;
        this.addDiv = true;
        const variablePackId = row.variablePackId || this.ids;
        // 查询变量分类
        this.getVariableClassificationOptions();
        // 查询数据结果表
        this.getResultTable();
        getManager(variablePackId).then(response => {
          this.form = response.data;
          this.detailViem = true;
          this.form.sourceTableValue = [];
          this.form.variableId = JSON.parse(this.form.variableId);
          this.selectVariableNote = JSON.parse(this.form.selectVariableNoteForm);
          this.form.originalVariable = JSON.parse(this.form.originalVariable);
          this.variableClassificationChange(this.form.variableClassification);
        });
      },

      /** 修改按钮操作 */
      handleUpdate(row) {
        this.detailViem = false;
        this.reset();
        this.title = "修改变量包管理";
        this.layoutOne = false;
        this.versionNumShow = true;
        this.UnAllowedUpdate = true;
        this.addDiv = true;
        const variablePackId = row.variablePackId || this.ids
        // 查询变量分类
        this.getVariableClassificationOptions();
        // 查询数据结果表
        this.getResultTable();
        getManager(variablePackId).then(response => {
          this.form = response.data;
          this.form.sourceTableValue = [];
          this.form.variableId = JSON.parse(this.form.variableId);
          console.log("-----this.form.variableId---");
          console.log(this.form.variableId);
          this.selectVariableNote = JSON.parse(this.form.selectVariableNoteForm);

          this.form.originalVariable = JSON.parse(this.form.originalVariable);
          this.variableClassificationChange(this.form.variableClassification);

          setTimeout(()=>{
            this.variableClassificationOptions.map((data, i) => {
              if (data.value === this.form.variableClassification) {
                this.sourceDataChange(data.sourceTableName);
                this.getDimensionRelationFild(data.dimensionRelation);
                // 数据源表赋值
                this.form.sourceTableName = data.sourceTableName;
                this.form.dimensionRelation = data.dimensionRelation;
              }
            });
          },300);
        });


      }
      ,
      /** 提交按钮 */
      submitForm: function () {
        this.$refs["form"].validate(valid => {
          if (valid) {
            this.form.selectVariableNoteForm = JSON.stringify(this.selectVariableNote);
            console.log("333");
            console.log(this.selectVariableNote);
            if (this.form.variablePackId !== undefined) {
              updateManager(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.getList();
                  this.layoutOne = true;
                  this.addDiv = false;
                } else {
                  this.msgError(response.msg);
                }

              });
            } else {
              // this.form.variableId = JSON.stringify(this.form.variableId);
              this.form.selectVariableNoteForm = JSON.stringify(this.selectVariableNote);
              addManager(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("新增成功");
                  this.getList();
                  this.layoutOne = true;
                  this.addDiv = false;
                } else {
                  this.msgError(response.msg);
                }
              });
            }
          }
        });
      }
      ,
      /** 删除按钮操作 */
      handleDelete(row) {
        const variablePackIds = row.variablePackId || this.ids;
        this.$confirm('是否确认删除变量包管理编号为"' + variablePackIds + '"的数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return delManager(variablePackIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function () {
        });
      }
      ,
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有变量包管理数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return exportManager(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function () {
        });
      }
    }
  }
  ;
</script>
<style>
  /*穿梭框高度宽度设置*/
  .el-transfer-panel {
    width: 490px;
    height: 450px;
  }

  .el-transfer-panel__list.is-filterable {
    height: 450px;
  }

  /*穿梭框空行问题*/
  .el-checkbox.el-transfer-panel__item {
    display: block;
  }

  .el-dialog {
    display: flex;
    flex-direction: column;
    margin: 0 !important;
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    /*height:600px;*/
    max-height: calc(100% - 30px);
    max-width: calc(100% - 30px);
  }

  .el-dialog .el-dialog__body {
    flex: 1;
    overflow: auto;
  }

  .customWidth {
    width: 80%;
  }
</style>
