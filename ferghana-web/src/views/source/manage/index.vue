<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="100px">
      <el-form-item label="数据源中文名" prop="dataSourceName">
        <el-input
          v-model="queryParams.dataSourceName"
          placeholder="请输入数据源中文名"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="数据源英文名" prop="tableName">
        <el-input
          v-model="queryParams.tableName"
          placeholder="请输入数据源英文名"
          clearable
          size="small"
          @keyup.enter.native="handleQuery"
        />
      </el-form-item>
      <el-form-item label="连接器类型" prop="connectorType" label-width="100px">
        <el-select
          v-model="queryParams.connectorType"
          placeholder="请选择连接器类型"
          clearable
          size="small"
        >
          <el-option
            v-for="dictSource in connectorTypeOptions"
            :key="dictSource.dictValue"
            :label="dictSource.dictLabel"
            :value="dictSource.dictValue"
          />
        </el-select>
      </el-form-item>

      <el-form-item style="padding-left: 12px">
        <el-button type="primary" icon="el-icon-search" v-hasPermi="['source:manage:query']" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" v-hasPermi="['source:manage:query']" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-plus"
          size="mini"
          @click="handleAdd"
          v-hasPermi="['source:manage:add']"
        >新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          v-hasPermi="['source:manage:remove']"
          @click="handleDelete"
        >批量删除
        </el-button>
      </el-col>
    </el-row>

    <el-table v-loading="loading" :data="sourceList" @selection-change="handleSelectionChange"  @row-dblclick="handleDetail">
      <el-table-column type="selection" width="45" align="center"/>
      <el-table-column label="数据源中文名" align="left" prop="dataSourceName"/>
      <el-table-column label="数据源英文名" align="left" prop="tableName"/>
      <el-table-column
        label="连接器类型"
        align="center"
        prop="connectorType"
        :formatter="connectorTypeMatter"
      />
      <el-table-column label="数据来源" align="left" prop="dataSource"/>
      <el-table-column label="新增人" align="center" width="130" prop="createBy"/>
      <el-table-column label="修改人" align="center" width="130" prop="updateBy"/>
      <el-table-column label="新增时间" align="center" prop="createTime" width="170">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="updateTime" width="170">
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.updateTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column
        label="操作"
        align="center"
        width="250"
        class-name="small-padding fixed-width"
      >
        <template slot-scope="scope">
          <el-button
            size="mini"
            type="text"
            @click="handleDetail(scope.row)"
          >详情
          </el-button>
          <el-button
            size="mini"
            type="text"
            @click="handleUpdate(scope.row)"
            v-hasPermi="['source:manage:edit']"
          >修改
          </el-button>
          <el-button
            v-if="scope.row.userId !== 1"
            size="mini"
            type="text"
            @click="handleDelete(scope.row)"
            v-hasPermi="['source:manage:remove']"
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

    <!-- 添加或修改数据源表对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="1100px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="130px" class="el-col-24">
        <el-form-item label="数据源中文名" prop="dataSourceName" class="el-col-12" >
          <el-input v-model="form.dataSourceName" placeholder="请输入数据源中文名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="数据源英文名" prop="tableName" class="el-col-12">
          <el-input v-model="form.tableName" placeholder="请输入数据源英文名" :disabled="detailViem"/>
        </el-form-item>
        <el-form-item label="连接器类型" prop="connectorType" class="el-col-12">
          <el-select
            v-model="form.connectorType"
            placeholder="请选择连接器类型"
            clearable
            style="width: 100%"
            :disabled="detailViem"
            @change="connectorTypeChange"
          >
            <el-option
              v-for="dict in connectorTypeOptions"
              :key="dict.dictValue"
              :label="dict.dictLabel"
              :value="dict.dictValue"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="数据来源" prop="dataSource" class="el-col-12">
          <el-input v-model="form.dataSource" placeholder="请输入数据来源" :disabled="detailViem"/>
        </el-form-item>
        <div v-show="connShow">
          <el-form-item label="topic名" prop="topicName" class="el-col-12">
            <el-input v-model="form.topicName" placeholder="请输入topic名" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="消费组" prop="consumerGroup" class="el-col-12">
            <el-input v-model="form.consumerGroup" placeholder="请输入消费组" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="消费模式" prop="consumerMode" class="el-col-12">
            <el-select v-model="form.consumerMode" placeholder="请选择消费模式" clearable style="width: 100%"
                       :disabled="detailViem">
              <el-option v-for="dict in consumerModeOptions" :key="dict.dictValue" :label="dict.dictLabel"
                         :value="dict.dictValue"/>
            </el-select>
          </el-form-item>
          <el-form-item label="zookeeper地址" prop="zookeeperAddress" class="el-col-12">
            <el-input
              v-model="form.zookeeperAddress"
              placeholder="master:2181,slave:2181"
              :disabled="detailViem"
            />
          </el-form-item>
          <el-form-item label="kafka地址" prop="kafkaAddress" class="el-col-12">
            <el-input v-model="form.kafkaAddress" placeholder="master:9092,slave:9092" :disabled="detailViem"/>
          </el-form-item>
        </div>
        <div v-show="!connShow" class="el-col-24">
          <el-form-item :label="mysqlIPFlag?'ip地址':'URL地址'" prop="myAddress" class="el-col-12">
            <el-input v-model="form.myAddress" :placeholder="mysqlIPFlag?'请输入ip地址':'请输入URL地址'" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="端口" prop="port" class="el-col-12" v-show="mysqlPropertyShow">
            <el-input v-model="form.port" placeholder="请输入端口号"  :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="用户名" prop="userName" class="el-col-12">
            <el-input v-model="form.userName" placeholder="请输入用户名" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="密码" prop="password" class="el-col-12">
            <el-input v-model="form.password" placeholder="请输入密码" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="数据库" prop="myDatabase" class="el-col-12" v-show="mysqlPropertyShow">
            <el-input v-model="form.myDatabase" placeholder="请输入数据库" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="表名" prop="myTableName" class="el-col-12">
            <el-input v-model="form.myTableName" placeholder="请输入表名" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item label="是否全表扫描" prop="scanAll" class="el-col-12">
            <el-radio-group v-model="form.scanAll" :disabled="detailViem">
              <el-radio label="1">是</el-radio>
              <el-radio label="0">否</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item label="数据操作" prop="handleData" class="el-col-12">
            <el-checkbox-group v-model="form.handleData" :disabled="detailViem">
              <el-checkbox label="I">insert</el-checkbox>
              <el-checkbox label="U">update</el-checkbox>
              <el-checkbox label="D">delete</el-checkbox>
            </el-checkbox-group>
          </el-form-item>
        </div>

        <el-form-item label="描述" prop="description" class="el-col-12">
          <el-input v-model="form.description" placeholder="请输入描述" :disabled="detailViem"/>
        </el-form-item>

        <div v-for="(item, index) in form.dynamicItem" :key="index" class="el-col-24">
          <el-form-item label="schema" class="el-col-5" :id="'label' + index"
                        :prop="'dynamicItem.' + index + '.schemaDefine'"
                        :rules="rules.dynamicItem.schemaDefine">
              <el-input v-model="item.schemaDefine" @change="schemaDefineChange(index)"
                        placeholder="请输入字段" @mouseover="mouseOver" style="width: 200px" :disabled="item.isUsed === '1' || detailViem"/>
          </el-form-item>
          <el-form-item class="el-col-6 elementStyle" style="padding-left: 100px"
                        :prop="'dynamicItem.' + index + '.dataBaseType'"
                        :rules="rules.dynamicItem.dataBaseType">
            <el-select v-model="item.dataBaseType" placeholder="请选择数据类型" clearable style="width: 220px"
                       :disabled="item.isUsed === '1' || detailViem">
              <el-option v-for="dict in sysDataBaseTypes" :key="dict.dictValue" :label="dict.dictLabel" :value="dict.dictValue"/>
            </el-select>
          </el-form-item>
          <el-form-item class="el-col-5"
                        :prop="'dynamicItem.' + index + '.schemaFieldName'"
                        :rules="rules.dynamicItem.schemaFieldName">
            <el-input v-model="item.schemaFieldName" placeholder="请输入中文名" style="width: 200px" :disabled="detailViem"/>
          </el-form-item>
          <el-form-item class="el-col-2 elementStyle" :prop="'dynamicItem.' + index + '.primaryKey'">
            <el-input v-model="item.primaryKey" :disabled="detailViem" v-show="false"/>
            <el-button @click="primaryKeyCheck(index)" :ref="'ref' + index" :id="'ref' + index"
                       :disabled="item.isUsed === '1' || detailViem"  style="padding: 10px; margin-left: 90px">主键
            </el-button>
          </el-form-item>
          <el-form-item class="el-col-6">
            <span>
              <el-button @click="addItem" :disabled="detailViem">
                <i class="el-icon-plus"/>
              </el-button>
              <el-button @click="deleteItem(item, index)" :disabled="item.isUsed === '1' || detailViem || form.dynamicItem.length === 1">
                <i class="el-icon-minus"/>
              </el-button>
            </span>
          </el-form-item>
        </div>
        <!-- 动态增加项目 -->
        <!-- 不止一项，用div包裹起来 -->
        <div class="el-col-24">
          <el-form-item label="watermark">
            <span><el-input v-model="form.waterMarkName" placeholder="请输入水印" style="width: 200px"
                            :disabled="detailViem"/></span>
            <span style="margin-left: 30px"><el-input value="TIMESTAMP(3)" style="width: 200px" disabled="disabled"/></span>
            <span style="margin-left: 30px"><el-input-number :disabled="detailViem" v-model="form.waterMarkTime" :min="0" :max="1000000"/></span>
            <span style="margin-left: 2px">秒</span>
          </el-form-item>
        </div>
      </el-form>

        <div slot="footer" class="dialog-footer">
          <el-button type="primary" @click="submitForm" v-show="showSubmitForm">确 定</el-button>
          <el-button @click="cancel">取 消</el-button>
        </div>
    </el-dialog>
  </div>
</template>

<script>
  import {
    listSource,
    getSource,
    delSource,
    addSource,
    updateSource,
    exportSource
  } from "@/api/source/manage.js";
  import{isLegitimateName,unContainSpace} from "@/utils/validate.js";

  export default {
    name: "Manage",
    data() {
      return {
        // 遮罩层
        loading: true,
        // 选中数组
        ids: [],
        // 非单个禁用
        single: true,
        scanAll: undefined,
        // 非多个禁用
        multiple: true,
        // 总条数
        total: 0,
        // 数据源表表格数据
        sourceList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 数据源类型数据
        dataSourceTypeOptions: [],
        // 连接器类型
        connectorTypeOptions: [],
        // 消费模式类型
        consumerModeOptions: [],
        // 数据类型
        sysDataBaseTypes: [],
        // mysql-Port展示字段
        mysqlPropertyShow:false,
        // 当选择mysql-cdc时为true
        mysqlIPFlag:true,

        // dynamicItem: [{
        //   schemaDefine:"",
        //   dataBaseType:""
        // }],
        // 控制标签是否可修改
        detailViem: false,
        updateViem: false,
        connShow: true,
        // 控制【确认】按钮是否显示
        showSubmitForm: true,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          dataSourceId: undefined,
          dataSourceName: undefined,
          dataSourceType: undefined,
          connectorType: undefined,
          dataSource: undefined,
          topicName: undefined,
          tableName: undefined,
          consumerGroup: undefined,
          consumerMode: undefined,
          zookeeperAddress: undefined,
          kafkaAddress: undefined,
          schemaDefine: undefined,
          dataBaseType: undefined,
          description: undefined
        },
        // 表单参数
        form: {},
        // 表单校验
        rules: {
          dataSourceName: [
            {required: true, message: "数据源名称不能为空", trigger: "blur"},
            {validator: unContainSpace, message: "数据源名称不能包含空格", trigger: "blur"}
          ],
          dataSourceType: [
            {required: true, message: "数据源类型不能为空", trigger: "blur"}
          ],
          connectorType: [
            {required: true, message: "连接器类型不能为空", trigger: "blur"}
          ],
          dataSource: [
            {required: true, message: "数据来源不能为空", trigger: "blur"}
          ],
          topicName: [
            {required: true, message: "topic名不能为空", trigger: "blur"}
          ],
          tableName: [
            {required: true, message: "表名不能为空", trigger: "blur"},
            {validator: isLegitimateName, message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母", trigger: "blur"}
          ],
          consumerGroup: [
            {required: true, message: "消费组不能为空", trigger: "blur"}
          ],
          consumerMode: [
            {required: true, message: "消费模式不能为空", trigger: "blur"}
          ],
          zookeeperAddress: [
            {required: true, message: "zookeeper地址不能为空", trigger: "blur"}
          ],
          kafkaAddress: [
            {required: true, message: "kafka地址不能为空", trigger: "blur"}
          ],
          myAddress: [
            {required: true, message: "地址不能为空", trigger: "blur"}
          ],
          port: [
            {required: true, message: "端口不能为空", trigger: "blur"}
          ],
          userName: [
            {required: true, message: "用户名不能为空", trigger: "blur"}
          ],
          password: [
            {required: true, message: "密码不能为空", trigger: "blur"}
          ],
          myDatabase: [
            {required: true, message: "数据库不能为空", trigger: "blur"}
          ],
          myTableName: [
            {required: true, message: "表名不能为空", trigger: "blur"}
          ],
          scanAll: [
            {required : true, message: "是否全表扫描必须选择", trigger: "blur"}
          ],
          handleData: [
            {required : true, message: "数据操作必须选择", trigger: "blur"}
          ],
          dynamicItem: {
            schemaDefine: [
              {required: true, message: "字段不能为空", trigger: "blur"},
              {validator: isLegitimateName, message: "格式不合法", trigger: "blur"}
            ],
            dataBaseType: [
              {required: true, message: "数据类型不能为空", trigger: "blur"}
            ],
            schemaFieldName: [
              {required: true, message: "中文名不能为空", trigger: "blur"}
            ]
          }
        }
      };
    },
    created() {
      this.getList();
      this.getDicts("sys_source_type").then(response => {
        this.dataSourceTypeOptions = response.data;
      });
      this.getDicts("sys_connector_type").then(response => {
        this.connectorTypeOptions = response.data;
      });
      this.getDicts("sys_consumer_mode").then(response => {
        this.consumerModeOptions = response.data;
      });
      this.getDicts("datasource_schema_type").then(response => {
        this.sysDataBaseTypes = response.data;
      });
    },
    methods: {

      connectorTypeChange(value){
        console.log("---connectorTypeChange");
        console.log(value);
        // 控制显示kafka或mysql板块
        this.connShow = value === '01';
        //  控制kafka或mysql板块的必输项
        if (value === '01') { // kafka
          this.rules.topicName[0].required = true;
          this.rules.topicName[0].validator = isLegitimateName;
          this.rules.consumerGroup[0].required = true;
          this.rules.consumerMode[0].required = true;
          this.rules.kafkaAddress[0].required = true;
          this.rules.zookeeperAddress[0].required = true;
          this.rules.myAddress[0].required = false;
          this.rules.port[0].required = false;
          this.rules.userName[0].required = false;
          this.rules.password[0].required = false;
          this.rules.myDatabase[0].required = false;
          this.rules.myTableName[0].required = false;
          this.rules.scanAll[0].required = false;
          this.rules.handleData[0].required = false;
        } else if (value === '02'){
          this.mysqlPropertyShow = true;
          this.rules.topicName[0].required = false;
          this.rules.topicName[0].validator = false;
          this.rules.consumerGroup[0].required = false;
          this.rules.consumerMode[0].required = false;
          this.rules.kafkaAddress[0].required = false;
          this.rules.zookeeperAddress[0].required = false;
          this.rules.myAddress[0].required = true;
          this.rules.port[0].required = true;
          this.rules.userName[0].required = true;
          this.rules.password[0].required = true;
          this.rules.myDatabase[0].required = true;
          this.rules.myTableName[0].required = true;
          this.rules.scanAll[0].required = true;
          this.rules.handleData[0].required = true;
          this.mysqlIPFlag = true;
        } else if (value === '03'){
          this.mysqlPropertyShow = false;
          this.rules.topicName[0].required = false;
          this.rules.topicName[0].validator = false;
          this.rules.consumerGroup[0].required = false;
          this.rules.consumerMode[0].required = false;
          this.rules.kafkaAddress[0].required = false;
          this.rules.zookeeperAddress[0].required = false;
          this.rules.myAddress[0].required = true;
          this.rules.port[0].required = false;
          this.rules.userName[0].required = true;
          this.rules.password[0].required = true;
          this.rules.myDatabase[0].required = false;
          this.rules.myTableName[0].required = true;
          this.rules.scanAll[0].required = true;
          this.rules.handleData[0].required = true;
          this.mysqlIPFlag = false;
        }
      },

      mouseOver(){
        console.log("==");

      },

      schemaDefineChange(index){
        if (this.form.dynamicItem[index].primaryKey !== "") {
          this.form.dynamicItem[index].primaryKey = this.form.dynamicItem[index].schemaDefine;
          this.form.schemaPrimaryKey = this.form.dynamicItem[index].schemaDefine;
        }
      },

      // 主键选择
      primaryKeyCheck(index) {
        // 先删除其他的所有的颜色
        for (let i = 0; i < this.form.dynamicItem.length; i++) {
          let dom = document.querySelector('#ref' + i);
          dom.style.backgroundColor = "#ffffff";
          dom.style.color = "#C0C4CC";
          if (index !== i) {
            this.form.dynamicItem[i].primaryKey = "";
          }
        }

        // 点击当前这个按钮变背景颜色
        var dom1 = document.querySelector('#ref' + index);
        // 如果已经为主键，则先取消
        if (this.form.dynamicItem[index].primaryKey !== "") {
          dom1.style.backgroundColor = "#ffffff";
          this.form.dynamicItem[index].primaryKey = "";
        } else {
          dom1.style.backgroundColor = "#1890ff";
          dom1.style.color = "#ffffff";
          // 赋值
          if (this.form.dynamicItem[index].schemaDefine === "") {
            this.form.dynamicItem[index].primaryKey = "1";
          } else {
            this.form.dynamicItem[index].primaryKey = this.form.dynamicItem[index].schemaDefine;
            this.form.schemaPrimaryKey = this.form.dynamicItem[index].schemaDefine;
          }
        }
      },

      // 增加行
      addItem() {
        this.form.dynamicItem.push({
          schemaDefine: "",
          dataBaseType: "",
          schemaFieldName: "",
          primaryKey: ""
        });
        // 增加的删除schema的label
        this.$nextTick(function () {
          for (let i = 1; i < this.form.dynamicItem.length; i++) {
            document.querySelector('#label' + i).firstElementChild.innerHTML = "";
          }
        })
      },
      // 删除行
      deleteItem(item, index) {
        if (this.form.dynamicItem && this.form.dynamicItem.length === 1) {
          this.$alert("不能删除所有的字段", {type: "error"});
          return false;
        }
        this.form.dynamicItem.splice(index, 1);
      },
      // 数据源类型字典翻译
      statusDataSourceType(row, column) {
        return this.selectDictLabel(
          this.dataSourceTypeOptions,
          row.dataSourceType
        );
      },
      // 连接器类型字典翻译
      connectorTypeMatter(row, column) {
        return this.selectDictLabel(this.connectorTypeOptions, row.connectorType);
      },
      /** 查询数据源表列表 */
      getList() {
        this.loading = true;
        listSource(this.queryParams).then(response => {
          this.sourceList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 取消按钮
      cancel() {
        this.open = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          dataSourceId: undefined,
          dataSourceName: undefined,
          dataSourceType: undefined,
          connectorType: undefined,
          dataSource: undefined,
          topicName: undefined,
          tableName: undefined,
          consumerGroup: undefined,
          consumerMode: undefined,
          zookeeperAddress: undefined,
          kafkaAddress: undefined,
          schemaDefine: undefined,
          description: undefined,
          schemaPrimaryKey: undefined,
          waterMarkName: undefined,
          waterMarkTime: undefined,
          handleData: [],
          dynamicItem: [
            {
              schemaDefine: "",
              dataBaseType: "",
              schemaFieldName: "",
              primaryKey: ""
            }
          ]
        };
        this.resetForm("form");
        this.connShow = true;
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
        this.ids = selection.map(item => item.dataSourceId);
        this.single = selection.length !== 1;
        this.multiple = !selection.length;
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.reset();
        this.detailViem = false;
        this.updateViem = false;
        this.showSubmitForm = true;
        this.open = true;
        this.title = "添加数据源";
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.reset();
        const dataSourceId = row.dataSourceId || this.ids;
        getSource(dataSourceId).then(response => {
          this.form = response.data;
          this.open = true;
          this.detailViem = false;
          this.updateViem = true;
          this.showSubmitForm = true;
          this.title = "修改数据源";
          this.connectorTypeChange(this.form.connectorType);
          this.$nextTick(function () {
            for (let i = 0; i < this.form.dynamicItem.length; i++) {
              if (this.form.dynamicItem[i].primaryKey !== "" && this.form.dynamicItem[i].primaryKey === this.form.schemaPrimaryKey) {
                let dom1 = document.querySelector('#ref' + i);
                dom1.style.backgroundColor = "#1890ff";
                dom1.style.color = "#ffffff";
              }
            }
          })
        });
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        console.log("============");
        this.reset();
        const dataSourceId = row.dataSourceId || this.ids;
        getSource(dataSourceId).then(response => {
          this.form = response.data;
          this.open = true;
          this.detailViem = true;
          this.updateViem = true;
          this.showSubmitForm = false;
          this.title = "查看数据源";
          this.connectorTypeChange(this.form.connectorType);
          this.$nextTick(function () {
            for (let i = 0; i < this.form.dynamicItem.length; i++) {
              if (this.form.dynamicItem[i].primaryKey !== "" && this.form.dynamicItem[i].primaryKey === this.form.schemaPrimaryKey) {
                console.log("+++");
                let dom1 = document.querySelector('#ref' + i);
                dom1.style.backgroundColor = "#1890ff";
                dom1.style.color = "#ffffff";
              }
            }
          })
        });
      },
      /** 提交按钮 */
      submitForm: function () {
        console.log("--submitForm");
        console.log(this.form);
        this.$refs["form"].validate(valid => {
          if (valid) {
            if (this.form.dataSourceId !== undefined) {
              console.log("--11---");
              console.log(this.form);
              updateSource(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            } else {
              // primaryKey
              for (let i = 0; i < this.form.dynamicItem.length; i++) {
                this.$delete(this.form.dynamicItem[i], "primaryKey");
              }
              // this.form.dynamicItem = JSON.toString(this.form.dynamicItem);
              addSource(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("新增成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            }
          }
        });
      },
      /** 删除按钮操作 */
      handleDelete(row) {
        const dataSourceIds = row.dataSourceId || this.ids;
        this.$confirm("是否确认删除该数据项?", "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        })
          .then(function () {
            return delSource(dataSourceIds);
          })
          .then(() => {
            this.getList();
            this.msgSuccess("删除成功");
          })
          .catch(function () {
          });
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm("是否确认导出所有数据源表数据项?", "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        })
          .then(function () {
            return exportSource(queryParams);
          })
          .then(response => {
            this.download(response.msg);
          })
          .catch(function () {
          });
      }
    }
  };
</script>

<style scoped>
  .elementStyle >>> .el-form-item__content {
    margin-left: 45px !important;
  }
</style>
