<template>
  <div class="app-container">
    <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="68px">
      <el-form-item label="变量分类" prop="variableClassificationName">
        <el-input v-model="queryParams.variableClassificationName" placeholder="请输入变量分类" clearable size="small"
          @keyup.enter.native="handleQuery" />
      </el-form-item>
      <el-form-item>
        <el-button type="primary" icon="el-icon-search" size="mini" @click="handleQuery">搜索</el-button>
        <el-button icon="el-icon-refresh" size="mini" @click="resetQuery">重置</el-button>
      </el-form-item>
    </el-form>

    <el-row :gutter="10" class="mb8">
      <el-col :span="1.5">
        <el-button type="primary" icon="el-icon-plus" size="mini" @click="handleAdd">新增
        </el-button>
      </el-col>
      <el-col :span="1.5">
        <el-button
          type="primary"
          icon="el-icon-delete"
          size="mini"
          :disabled="multiple"
          @click="handleDelete"
        >批量删除
        </el-button>
      </el-col>
    </el-row>

    <el-table v-loading="loading" :data="classificationList" @selection-change="handleSelectionChange" @row-dblclick="handleDetail">
      <el-table-column type="selection" width="55" align="center" />
      <el-table-column label="变量分类名" align="left" prop="variableClassificationName" />
      <el-table-column label="新增人" align="center"  prop="createBy"/>
      <el-table-column label="修改人" align="center"  prop="updateBy"/>
      <el-table-column label="新增时间" align="center" prop="createTime" >
        <template slot-scope="scope">
          <span>{{ parseTime(scope.row.createTime) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="修改时间" align="center" prop="updateTime" >
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
            >修改
            </el-button>
            <el-button
              v-if="scope.row.userId !== 1"
              size="mini"
              type="text"
              @click="handleDelete(scope.row)"
            >删除
            </el-button>
          </template>
        </el-table-column>
    </el-table>

    <pagination v-show="total>0" :total="total" :page.sync="queryParams.pageNum" :limit.sync="queryParams.pageSize"
      @pagination="getList" />

    <!-- 添加或修改变量分类对话框 -->
    <el-dialog :title="title" :visible.sync="open" width="900px" :close-on-click-modal="false">
      <el-form ref="form" :model="form" :rules="rules" label-width="140px">
        <el-form-item label="变量分类名" prop="variableClassificationName">
          <el-input v-model="form.variableClassificationName" placeholder="请输入变量分类名" style="width: 100%"
            :disabled="detailViem" />
        </el-form-item>

        <!-- 数据源表选择框 -->
        <div class="el-col-24">
          <!-- 第一个数据源表 -->
          <el-form-item :label="(sourceTwoDabItem ? '数据源表(一)' : '数据源表')" prop="sourceDabRelation" class="el-col-20">
            <el-select v-model="form.sourceDabRelation" placeholder="请选择数据源表" @change="sourceDabRelationChange"
              style="width: 100%" :disabled="detailViem">
              <el-option v-for="data in sourceDataOptions" :key="data.value" :label="data.name" :value="data.value" />
            </el-select>
          </el-form-item>

          <el-form-item class="el-col-4 elementStyle">
            <span>
              <!-- 第一个数据源表只能加操作 -->
              <el-button @click="sourceAddItem" :disabled="detailViem">
                <i class="el-icon-plus" />
              </el-button>
              <el-button :disabled="true">
                <i class="el-icon-minus" />
              </el-button>
            </span>
          </el-form-item>

          <!-- 第二个数据源表 -->
          <el-form-item label="数据源表(二)" :prop="sourceTwoDabItem ? 'sourceTwoDabRelation' : ''" class="el-col-20" v-if="sourceTwoDabItem">
            <el-select v-model="form.sourceTwoDabRelation" placeholder="请选择数据源表" @change="sourceTwoDabRelationChange" no-data-text="请先选择数据源表(一)"
              style="width: 100%" :disabled="detailViem">
              <el-option v-for="data in sourceTwoDataOptions" :key="data.value" :label="data.name" :value="data.value" />
            </el-select>
          </el-form-item>

          <el-form-item class="el-col-4 elementStyle" v-if="sourceTwoDabItem">
            <span>
              <!-- 第二个数据源表只能减操作 -->
              <el-button :disabled="true">
                <i class="el-icon-plus" />
              </el-button>
              <el-button @click="sourceDeleteItem" :disabled="detailViem">
                <i class="el-icon-minus" />
              </el-button>
            </span>
          </el-form-item>

          <!-- 数据源表关联字段 -->
          <el-form-item label="源表(一)关联字段" :prop="sourceTwoDabItem ? 'sourceRelation.sourceDabField' : ''" class="el-col-12" v-if="sourceTwoDabItem">
            <el-select v-model="form.sourceRelation.sourceDabField" placeholder="请选择源表(一)关联字段" @change="refresh"
              style="width: 100%" :disabled="detailViem" no-data-text="请先选择源表(一)">
              <el-option v-for="dict in sourceDabFieldOptions" :key="dict.value" :label="dict.label"
                :value="dict.value" />
            </el-select>
          </el-form-item>

          <el-form-item label="源表(二)关联字段" :prop="sourceTwoDabItem ? 'sourceRelation.sourceTwoDabField' : ''" class="el-col-12" v-if="sourceTwoDabItem">
            <el-select v-model="form.sourceRelation.sourceTwoDabField" placeholder="请选择源表(二)关联字段" @change="refresh" no-data-text="请先选择源表(二)" style="width: 100%"
              :disabled="detailViem">
              <el-option v-for="data in sourceTwoDabFieldOptions" :key="data.value" :label="data.name"
                :value="data.value"/>
            </el-select>
          </el-form-item>

          <el-form-item label="关联时间范围" class="el-col-24" v-if="sourceTwoDabItem" :rules="[{ required: true}]">
            <el-form-item class="el-col-11" :prop="sourceTwoDabItem ? 'sourceRelation.lowScope' : ''" v-if="sourceTwoDabItem">
              <el-input-number v-model="form.sourceRelation.lowScope" controls-position="right" :min="0" style="width: 100%;" :validate-event="true" :disabled="detailViem"/>
            </el-form-item>
            <el-form-item class="el-col-2" v-if="sourceTwoDabItem">
              <el-col class="line" style="text-align: center;">-</el-col>
            </el-form-item>
            <el-form-item class="el-col-11" :prop="sourceTwoDabItem ? 'sourceRelation.highScope' : ''" v-if="sourceTwoDabItem">
              <el-input-number v-model="form.sourceRelation.highScope" controls-position="right" :min="0" style="width: 100%;" :validate-event="true" :disabled="detailViem"/>
            </el-form-item>
          </el-form-item>

        </div>

        <div v-for="(item, index) in form.dimensionRelation" :key="index">
          <div class="el-col-24">
            <el-form-item label="关联数据维表" class="el-col-10" :rules="(item.sourceDabField == '' && item.dimensionName == '' ? rules.dimensionRelation.dimensionName : rules.dimensionRelationNotNull.dimensionName)"
              :prop="'dimensionRelation.'+index+'.dimensionName'">
              <el-select v-model="item.dimensionName" placeholder="请选择关联数据维表" @change="dimensionDabRelationChange"
                style="width: 100%" :disabled="detailViem" :id="'label' + index">
                <el-option v-for="dict in dimensionRelationOptions" :key="dict.value" :label="dict.label"
                  :value="dict.value+','+dict.connectorType+','+index" />
              </el-select>
            </el-form-item>

            <el-form-item label="数据源表关联字段" class="el-col-10" v-show="!judgeEs(item.relation)"
              :rules="judgeEs(item.relation) ? 'null' : (item.sourceDabField == '' && item.dimensionName == '' ? rules.dimensionRelation.sourceDabField : rules.dimensionRelationNotNull.sourceDabField)" :prop="'dimensionRelation.'+index+'.sourceDabField'">
              <el-select v-model="item.sourceDabField" placeholder="请选择关联数据源表字段"
                @change="sourceDabFieldChange" no-data-text="请先选择数据源表" style="width: 100%"
                :disabled="detailViem">
                <el-option v-for="data in sourceDabFieldOptions" :key="data.value" :label="data.name"
                  :value="'sourceDabField,'+data.value"  v-if="!sourceTwoDabItem"/>
                <el-option-group :label="form.sourceDabRelation" v-if="sourceTwoDabItem">
                  <el-option v-for="data in sourceDabFieldOptions" :key="data.value" :label="data.name"
                    :value="'sourceDabField,'+data.value"/>
                </el-option-group>
                <el-option-group :label="form.sourceTwoDabRelation" v-if="sourceTwoDabItem">
                  <el-option v-for="data in sourceTwoDabFieldOptions" :key="data.value" :label="data.name"
                    :value="'sourceTwoDabField,'+data.value"/>
                </el-option-group>
              </el-select>
            </el-form-item>

            <el-form-item class="el-col-4 elementStyle">
              <span>
                <el-button @click="dimensionAddItem" :disabled="detailViem">
                  <i class="el-icon-plus" />
                </el-button>
                <el-button @click="dimensionDeleteItem(item, index)" :disabled="detailViem || form.dimensionRelation.length === 1">
                  <i class="el-icon-minus" />
                </el-button>
              </span>
            </el-form-item>
          </div>

          <!-- 动态增加项目 -->
          <!-- 不止一项，用div包裹起来 -->
          <div v-for="(relation, index2) in item.relation" :key="index2" class="el-col-24" v-show="item.relation">

            <el-form-item label="数据维表关联字段" class="el-col-9" :rules="(item.dimensionName == '' ? rules.dimensionRelation.relation.dimensionDabField : rules.dimensionRelationNotNull.relation.dimensionDabField)"
              :prop="'dimensionRelation.'+index+'.relation.'+index2+'.dimensionDabField'">
              <el-select v-model="relation.dimensionDabField" placeholder="请选择数据维表关联字段" no-data-text="请先选择数据维表"
                style="width: 100%" :disabled="detailViem" @change="refresh()">
                <el-option v-for="data in dimensionDabFieldOptions" :key="data.value" :label="data.name"
                  :value="data.value"/>
              </el-select>
            </el-form-item>

            <el-form-item label="数据源表关联字段" class="el-col-9" :rules="(item.dimensionName == '' ? rules.dimensionRelation.relation.sourceDabField : rules.dimensionRelationNotNull.relation.sourceDabField)"
              :prop="'dimensionRelation.'+index+'.relation.'+index2+'.sourceDabField'">
              <el-select v-model="relation.sourceDabField" placeholder="请选择数据源表关联字段" no-data-text="请先选择数据源表"
                style="width: 100%" :disabled="detailViem" @change="sourceDabFieldChange">
                <el-option v-for="data in sourceDabFieldOptions" :key="data.value" :label="data.name"
                  :value="'sourceDabField,'+data.value"  v-if="!sourceTwoDabItem"/>
                <el-option-group :label="form.sourceDabRelation" v-if="sourceTwoDabItem">
                  <el-option v-for="data in sourceDabFieldOptions" :key="data.value" :label="data.name"
                    :value="'sourceDabField,'+data.value"/>
                </el-option-group>
                <el-option-group :label="form.sourceTwoDabRelation" v-if="sourceTwoDabItem">
                  <el-option v-for="data in sourceTwoDabFieldOptions" :key="data.value" :label="data.name"
                    :value="'sourceTwoDabField,'+data.value"/>
                </el-option-group>
              </el-select>
            </el-form-item>

            <el-form-item class="el-col-4 elementStyle">
              <span>
                <el-button @click="fieIdAddItem(index)" :disabled="detailViem">
                  <i class="el-icon-plus" />
                </el-button>
                <el-button @click="fieIdDeleteItem(item, index, index2)" :disabled="detailViem">
                  <i class="el-icon-minus" />
                </el-button>
              </span>
            </el-form-item>
          </div>


        </div>

        <div class="el-col-24">
          <el-form-item label="备注" prop="description">
            <el-input v-model="form.description" type="textarea" placeholder="请输入备注内容" style="width: 100%"
              :disabled="detailViem" />
          </el-form-item>
        </div>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="submitForm" :disabled="detailViem">确 定</el-button>
        <el-button @click="cancel">取 消</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
  import {
    listClassification,
    getClassification,
    delClassification,
    addClassification,
    updateClassification,
    exportClassification
  } from "@/api/variable/classification";
  import axios from "axios";
  import {
    getToken
  } from '@/utils/auth'

  export default {
    name: "Classification",
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
        // 变量分类表格数据
        classificationList: [],
        // 弹出层标题
        title: "",
        // 是否显示弹出层
        open: false,
        // 第二个数据源表选择框是否显示，默认false
        sourceTwoDabItem: false,
        // 数据源表options
        sourceDataOptions: [],
        // 数据源表(二)options
        sourceTwoDataOptions: [],
        // 数据维表options
        dimensionDataOptions: [],
        // 数据源表关联字段options
        sourceDabFieldOptions: [],
         // 数据源表(二)关联字段options
        sourceTwoDabFieldOptions: [],
        // 数据维表关联字段options
        dimensionDabFieldOptions: [],
        detailViem: false,
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          variableClassificationName: undefined,
          sourceDabRelation: undefined,
          description: undefined,
        },
        // 表单参数
        form: {},
        // 表单校验
        rules: {
          variableClassificationName: [{
            required: true,
            message: "变量分类名不能为空",
            trigger: "blur"
          }],
          sourceDabRelation: [{
            required: true,
            message: "数据源表不能为空",
            trigger: "blur"
          }],
          sourceTwoDabRelation: [{
            required: true,
            message: "数据源表不能为空",
            trigger: "blur"
          }],
          dimensionDabRelation: [{
            required: true,
            message: "关联数据维表不能为空",
            trigger: "blur"
          }],
          sourceRelation:{
            sourceDabField:[{
              required: true,
              message: "源表(一)关联字段不能为空",
              trigger: "blur"
            }],
            sourceTwoDabField:[{
              required: true,
              message: "源表(二)关联字段不能为空",
              trigger: "blur"
            }],
            lowScope:[{
              required: true,
              message: "关联时间范围不能为空",
              trigger: "blur"
            },{ type: 'number', message: '关联时间范围必须为数字',trigger: "blur"}],
            highScope:[{
              required: true,
              message: "关联时间范围不能为空",
              trigger: "blur"
            },{ type: 'number', message: '关联时间范围必须为数字',trigger: "blur"}]
          },
          dimensionRelation: {
            dimensionName: [{
              required: false,
              message: "数据维表不能为空",
              trigger: "blur"
            }],
            sourceDabField: [{
              required: false,
              message: "数据源表关联字段不能为空",
              trigger: "blur"
            }],
            relation: {
              sourceDabField: [{
                required: false,
                message: "数据源表关联字段不能为空",
                trigger: "blur"
              }],
              dimensionDabField: [{
                required: false,
                message: "数据维表关联字段不能为空",
                trigger: "blur"
              }]
            }
          },
          dimensionRelationNotNull: {
            dimensionName: [{
              required: true,
              message: "数据维表不能为空",
              trigger: "blur"
            }],
            sourceDabField: [{
              required: true,
              message: "数据源表关联字段不能为空",
              trigger: "blur"
            }],
            relation: {
              sourceDabField: [{
                required: true,
                message: "数据源表关联字段不能为空",
                trigger: "blur"
              }],
              dimensionDabField: [{
                required: true,
                message: "数据维表关联字段不能为空",
                trigger: "blur"
              }]
            }
          }
        }
      };
    },
    created() {
      this.getList();
    },
    watch:{
      sourceTwoDabItem(newVal){
        if(newVal && this.form.sourceTwoDabRelation && this.form.sourceTwoDabRelation !== ""){
          this.sourceTwoDabRelationChange(this.form.sourceTwoDabRelation);
        }
        if(!newVal){
          this.sourceTwoDabFieldOptions = [];
        }
      }
    },
    methods: {
      // jdbc增加行
      dimensionAddItem() {
        this.form.dimensionRelation.push({
          dimensionName: "",
          sourceDabField: "",
        });
      },
      // jdbc删除行
      dimensionDeleteItem(item, index) {
        if (this.form.dimensionRelation && this.form.dimensionRelation.length === 1) {
          this.$alert("不能删除所有的列！", {
            type: "error"
          });
          return false;
        }
        this.form.dimensionRelation.splice(index, 1);
      },
      // 数据源表增加行
      sourceAddItem(){
        this.sourceTwoDabItem = true;
      },
      // 数据源表减少行
      sourceDeleteItem(){
        this.sourceTwoDabItem = false;
        this.form.sourceTwoDabRelation = undefined;
        this.form.sourceRelation = {
          sourceDabField:"",
          sourceTwoDabField:"",
          lowScope:"",
          highScope:""
        }
        this.form.dimensionRelation.forEach((item)=>{
          if(item.sourceDabField){
            if(item.sourceTwoDabField){
              item.sourceDabField = "";
            }
          }
          else if(item.relation){
            item.relation.forEach((item2)=>{
              if(item2.sourceTwoDabField){
                item2.sourceDabField = "";
              }
            })
          }
        })
      },
      // 数据维表字段添加行
      fieIdAddItem(index) {
        this.form.dimensionRelation[index].relation.push({
          dimensionDabField: "",
          sourceDabField: ""
        });
        this.$forceUpdate();
      },
      // 数据维表字段减少行
      fieIdDeleteItem(item, index1, index2) {
        if (this.form.dimensionRelation[index1].relation && this.form.dimensionRelation[index1].relation.length === 1) {
          this.$alert("不能删除所有的列！", {
            type: "error"
          });
          return false;
        }
        this.form.dimensionRelation[index1].relation.splice(index2, 1);
        this.$forceUpdate();
      },
      // 数据源表切换
      sourceDabRelationChange(value) {
        this.form.sourceDabField = value;
        let that = this;
        if (value && value !== "") {
          // 不能选择两个相同的数据源表相关联
          this.sourceTwoDataOptions = [];
          for(let i=0;i<this.sourceDataOptions.length;i++){
            if(this.sourceDataOptions[i].value !== value){
              this.sourceTwoDataOptions.push(this.sourceDataOptions[i])
            }
          }
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/source/manage/querySchema',
            headers: {
              'Authorization': 'Bearer ' + getToken()
            },
            responseType: 'json',
            params: {
              dataSourceName: value
            },
          }).then(function(resp) {
            that.sourceDabFieldOptions = [];
            for (let i = 0; i < resp.data.rows[0].length; i++) {
              that.sourceDabFieldOptions.push({
                value: resp.data.rows[0][i].key,
                name: resp.data.rows[0][i].key,
              })
            }
            that.$forceUpdate();
          }).catch(resp => {
            console.log('获取数据源表请求失败!');
          });
        } else {
          that.sourceDabFieldOptions = [];
        }
      },
      // 数据源表(二)切换
      sourceTwoDabRelationChange(value) {
        this.form.sourceDabField = value;
        let that = this;
        if (value && value !== "") {
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/source/manage/querySchema',
            headers: {
              'Authorization': 'Bearer ' + getToken()
            },
            responseType: 'json',
            params: {
              dataSourceName: value
            },
          }).then(function(resp) {
            that.sourceTwoDabFieldOptions = [];
            for (let i = 0; i < resp.data.rows[0].length; i++) {
              that.sourceTwoDabFieldOptions.push({
                value: resp.data.rows[0][i].key,
                name: resp.data.rows[0][i].key,
              })
            }
            that.$forceUpdate();
          }).catch(resp => {
            console.log('获取数据源表请求失败!');
          });
        } else {
          that.sourceTwoDabFieldOptions = [];
        }
      },
      // 数据维表切换
      dimensionDabRelationChange(value) {
        this.form.dimensionDabField = "";
        let that = this;
        if (value !== "") {
          const connectorType = value.split(",")[1];
          const index = parseInt(value.split(",")[2]);
          value = value.split(",")[0];
          if (connectorType === "04" || connectorType === "05") {
            that.form.dimensionRelation[index] = {
              dimensionName: value,
              relation: [{
                dimensionDabField: "",
                sourceDabField: ""
              }]
            }
          } else {
            that.form.dimensionRelation[index] = {
              dimensionName: value,
              sourceDabField: ""
            }
          }
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/system/Dimension/querySchema',
            headers: {
              'Authorization': 'Bearer ' + getToken()
            },
            responseType: 'json',
            params: {
              dimensionNames: value
            },
          }).then(function(resp) {
            const data = resp.data.rows[0][`${Object.keys(resp.data.rows[0])[0]}`];
            that.dimensionDabFieldOptions = [];
            for (let i = 0; i < data.length; i++) {
              that.dimensionDabFieldOptions.push({
                value: data[i].key,
                name: data[i].key,
              })
            }
            that.$forceUpdate();
          }).catch(resp => {
            console.log('获取数据源表请求失败!');
          });
        } else {
          that.dimensionDabFieldOptions = [];
        }
      },
      // 数据源表关联字段切换
      sourceDabFieldChange(value) {
        this.form.dimensionRelation.forEach((item)=>{
          if(item.sourceDabField && item.sourceDabField === value){
            if(value.split(",")[0] === "sourceDabField"){
              item.sourceDabField = value.split(",")[1];
            }
            else if(value.split(",")[0] === "sourceTwoDabField"){
              item.sourceDabField = value.split(",")[1];
              item.sourceTwoDabField = value.split(",")[1];
            }
          }
          else if(item.relation){
            item.relation.forEach((item2)=>{
               if(item2.sourceDabField === value){
                 if(value.split(",")[0] === "sourceDabField"){
                   item2.sourceDabField = value.split(",")[1];
                 }
                 else if(value.split(",")[0] === "sourceTwoDabField"){
                   item2.sourceDabField = value.split(",")[1];
                   item2.sourceTwoDabField = value.split(",")[1];
                 }
               }
            })
          }
        })
        this.refresh();
      },
      // 查询数据维表
      getDimensionData() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/system/Dimension/list',
          headers: {
            'Authorization': 'Bearer ' + getToken()
          },
          responseType: 'json'
        }).then(function(resp) {
          console.log(resp.data)
          for (let i = 0; i < resp.data.rows.length; i++) {
            // 排除01 - redis ， redis 不作为维表关联
            if ('01' !== resp.data.rows[i].connectorType) {
              that.dimensionRelationOptions.push({
                value: resp.data.rows[i].dimensionName,
                label: resp.data.rows[i].dimensionName,
                connectorType: resp.data.rows[i].connectorType
              });
            }
          }
          that.$forceUpdate();
        }).catch(resp => {
          console.log('获取数据源表请求失败：' + resp.status + ',' + resp.statusText);
        });
      },
      // 判断是否为es表
      judgeEs(relation) {
        if (relation) {
          return true;
        }
        return false;
      },
      // 查询数据源表
      getSourceData() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/source/manage/list',
          headers: {
            'Authorization': 'Bearer ' + getToken()
          },
          responseType: 'json'
        }).then(function(resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            that.sourceDataOptions.push({
              value: resp.data.rows[i].tableName,
              name: resp.data.rows[i].tableName,
            });
          }
        }).catch(resp => {
          console.log('获取数据源表请求失败：' + resp.status + ',' + resp.statusText);
        });
      },
      /** 查询变量分类列表 */
      getList() {
        this.loading = true;
        listClassification(this.queryParams).then(response => {
          response.rows.forEach(item=>{
            item.sourceDab = item.sourceDabRelation + (item.sourceTwoDabRelation !== null && item.sourceTwoDabRelation !== "" ? "/" + item.sourceTwoDabRelation : "");
          })
          this.classificationList = response.rows;
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
          variableClassificationId: undefined,
          variableClassificationName: undefined,
          sourceDabRelation: undefined,
          sourceTwoDabRelation: undefined,
          description: undefined,
          createTime: undefined,
          updateTime: undefined,
          sourceDabField: undefined,
          dimensionRelation: [{
            dimensionName: "",
            sourceDabField: ""
          }],
          sourceRelation:{
            sourceDabField:"",
            sourceTwoDabField:"",
            lowScope:"",
            highScope:""
          }
        };
        this.resetForm("form");
        this.sourceTwoDabItem = false;
        this.sourceDataOptions = [];
        this.dimensionRelationOptions = [];
        this.sourceDabFieldOptions = [];
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
        this.ids = selection.map(item => item.variableClassificationId);
        this.single = selection.length !== 1;
        this.multiple = !selection.length
      },
      /** 新增按钮操作 */
      handleAdd() {
        this.detailViem = false;
        this.reset();
        this.open = true;
        this.title = "添加变量分类";
        this.getSourceData();
        this.getDimensionData();
        this.rules.sourceDabField[0].required = false;
      },
      /** 详情按钮操作 */
      handleDetail(row) {
        this.detailViem = true;
        this.reset();
        this.getSourceData();
        this.getDimensionData();
        const variableClassificationId = row.variableClassificationId || this.ids;
        getClassification(variableClassificationId).then(response => {
          this.form = response.data;
          let parse = JSON.parse(this.form.dimensionRelation);
          this.form.dimensionRelation = JSON.parse(this.form.dimensionRelation);
          this.form.sourceRelation = JSON.parse(this.form.sourceRelation);
          if(this.form.sourceTwoDabRelation && this.form.sourceTwoDabRelation !== ""){
            this.sourceTwoDabItem = true;
          }
          this.open = true;
          this.detailViem = true;
          this.title = "查看变量分类";
        });
      },
      /** 修改按钮操作 */
      handleUpdate(row) {
        this.reset();
        this.detailViem = false;
        this.getSourceData();
        this.getDimensionData();
        const variableClassificationId = row.variableClassificationId || this.ids
        getClassification(variableClassificationId).then(response => {
          this.form = response.data;
          this.form.dimensionRelation = JSON.parse(this.form.dimensionRelation);
          if(this.form.dimensionRelation.length === 0){
            this.form.dimensionRelation.push({
              dimensionName: "",
              sourceDabField: "",
            });
          }
          this.form.sourceRelation = JSON.parse(this.form.sourceRelation);
          if(this.form.sourceTwoDabRelation && this.form.sourceTwoDabRelation !== ""){
            this.sourceTwoDabItem = true;
          }
          this.open = true;
          this.title = "修改变量分类";
          let tmp = this.form.sourceDabField;
          this.form.sourceDabField = tmp;
          let that = this;
          const baseUrl = process.env.VUE_APP_BASE_API;
          this.sourceDabRelationChange(this.form.sourceDabRelation, true);
          this.dimensionDabRelationChange(this.form.dimensionDabRelation, true);
        });
      },
      /** 强制更新 */
      refresh(){
        this.dimensionAddItem();
        this.dimensionDeleteItem(null,this.form.dimensionRelation.length - 1);
        this.$forceUpdate()
      },
      /** 提交按钮 */
      submitForm: function() {
        this.refresh();
        this.$refs["form"].validate(valid => {
          if (valid) {
            // 删除空白数据维表行
            let temp = this.form.dimensionRelation;
            for(let i=temp.length-1;i>=0;i--){
              if(temp[i].dimensionName === "" || temp[i].sourceDabField === ""){
                this.$delete(temp,i)
              }
            }
            this.form.dimensionRelation = temp;
            if(this.form.sourceRelation && this.form.sourceRelation.lowScope !== "" && this.form.sourceRelation.highScope !== ""){
              if(this.form.sourceRelation.lowScope > this.form.sourceRelation.highScope){
                let temp = this.form.sourceRelation.lowScope;
                this.form.sourceRelation.lowScope = this.form.sourceRelation.highScope;
                this.form.sourceRelation.highScope = temp;
              }
            }
            if (this.form.variableClassificationId !== undefined) {
              updateClassification(this.form).then(response => {
                if (response.code === 200) {
                  this.msgSuccess("修改成功");
                  this.open = false;
                  this.getList();
                } else {
                  this.msgError(response.msg);
                }
              });
            } else {
              // this.form.dimensionRelation = JSON.stringify(this.form.dimensionRelation);
              addClassification(this.form).then(response => {
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
        const variableClassificationIds = row.variableClassificationId || this.ids;
        this.$confirm('是否确认删除变量分类编号为"' + variableClassificationIds + '"的数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function() {
          return delClassification(variableClassificationIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function() {});
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有变量分类数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function() {
          return exportClassification(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function() {});
      }
    }
  };
</script>
<style scoped>
  .elementStyle>>>.el-form-item__content {
    margin-left: 10px !important;
  }
</style>
