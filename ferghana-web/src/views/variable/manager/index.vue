<template>
  <div class="app-container">
    <div v-show="layoutOne">
      <el-form :model="queryParams" ref="queryForm" :inline="true" label-width="85px">
        <el-form-item label="变量中文名" prop="variableName">
          <el-input
            v-model="queryParams.variableName"
            placeholder="请输入变量中文名"
            clearable
            size="small"
            @keyup.enter.native="handleQuery"
          />
        </el-form-item>
        <el-form-item label="变量英文名" prop="variableNameEn">
          <el-input
            v-model="queryParams.variableNameEn"
            placeholder="请输入英文名"
            clearable
            size="small"
            @keyup.enter.native="handleQuery"
          />
        </el-form-item>
        <el-form-item label="变量分类" prop="variableClassificationName">
          <el-select v-model="queryParams.variableClassificationName" placeholder="请选择变量类型" clearable size="small">
            <el-option
              v-for="dict in variableClassificationOptions"
              :key="dict.name"
              :label="dict.name"
              :value="dict.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="变量类型" prop="variableType">
          <el-select v-model="queryParams.variableType" placeholder="请选择变量类型" clearable size="small">
            <el-option
              v-for="dict in variableTypeOptions"
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
            v-hasPermi="['variable:variable:add']"
          >新增
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

      <el-table v-loading="loading" :data="centerList" @selection-change="handleSelectionChange" @row-dblclick="versionDetail">
        <el-table-column type="selection" width="45" align="left" />
        <el-table-column label="变量中文名" align="left" header-align="left" prop="variableName"/>
        <el-table-column label="变量英文名"  width="150" align="left" prop="variableNameEn"/>
        <el-table-column label="变量分类"  width="200" align="left" prop="variableClassificationName"/>
        <el-table-column label="版本号" width="70" align="center" prop="versionNum"/>
        <el-table-column label="变量类型"  width="80" align="center" prop="variableType" :formatter="variableTypeFormat"/>
        <el-table-column label="操作人" align="center" width="130" prop="createBy"/>
        <el-table-column label="操作时间" align="center" prop="updateTime" width="170">
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
              @click="versionDetail(scope.row)"
            >详情
            </el-button>
            <el-button
              size="mini"
              type="text"
              @click="handleUpdate(scope.row)"
              v-hasPermi="['variable:variable:edit']"
            >修改
            </el-button>
            <el-button
              size="mini"
               type="text"
              @click="testVariabel(scope.row)"
            >测试
            </el-button>
            <el-button
              v-if="scope.row.userId !== 1"
              size="mini"
              type="text"
              @click="handleDelete(scope.row)"
              v-hasPermi="['variable:variable:remove']"
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

    <!-- 添加或修改变量管理中心对话框 -->
    <div v-show="addDiv">
      <el-form ref="form" :model="form" :rules="rules" label-width="120px" class="el-col-24" v-loading="updateLoading" >
        <div :style="{opacity: updateLoading ? '0' : '1'}">
          <el-form-item label="变量中文名" prop="variableName" class="el-col-12">
            <el-input v-model="form.variableName" placeholder="请输入变量中文名" :disabled="UnAllowedUpdate"/>
          </el-form-item>
          <el-form-item label="变量英文名" prop="variableNameEn" class="el-col-12">
            <el-input v-model="form.variableNameEn" placeholder="请输入变量英文名" :disabled="UnAllowedUpdate"/>
          </el-form-item>
          <el-form-item label="当前版本" prop="versionNum" class="el-col-12" v-show="versionNumShow">
            <el-input v-model="form.versionNum" placeholder="请输入变量英文名"  disabled="disabled"/>
          </el-form-item>
          <el-form-item label="变量分类" prop="variableClassification" class="el-col-12">
            <el-select v-model="form.variableClassification" placeholder="请选择变量类型" @change="variableClassificationChange"
                       clearable style="width: 100%" :disabled="UnAllowedUpdate">
              <el-option
                v-for="(data,index) in variableClassificationOptions"
                :key="index"
                :label="data.name"
                :value="data.value"
                :sourceId="data.sourceDabRelation"
                :dimensionRelationId="data.dimensionRelation"
                :dimensionName="data.dimensionName"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="变量类型" prop="variableType" class="el-col-12">
            <el-select v-model="form.variableType" placeholder="请选择变量类型" @change="variableTypeChange" clearable
                       style="width: 100%" :disabled="UnAllowedUpdate">
              <el-option
                v-for="(dict,index) in variableTypeOptions"
                :key="index"
                :label="dict.dictLabel"
                :value="dict.dictValue"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="模板类型" prop="variableModelType" v-show="variableModelTypeItem" class="el-col-12">
            <el-select v-model="form.variableModelType" @change="variableModelTypeChange" placeholder="请选择模板类型" clearable
                       style="width: 100%" :disabled="UnAllowedUpdate">
              <el-option
                v-for="dict in modelTypeOptions"
                :key="dict.dictValue"
                :label="dict.dictLabel"
                :value="dict.dictValue"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="变量备注" prop="description" class="el-col-24">
            <el-input v-model="form.description" type="textarea" placeholder="请输入变量备注" :disabled="detailViem"/>
          </el-form-item>

          <!--        普通查询展示模块        -->
          <div v-show="normalDiv">
            <el-form-item label="集群名" class="el-col-12" prop="clusterName">
              <el-select v-model="form.clusterName" placeholder="请选择集群名" clearable style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="data in dimensionDataOptions"
                  :key="data.value"
                  :label="data.label"
                  :value="data.value"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="redis方法" class="el-col-12" prop="redisFunction">
              <el-select v-model="form.redisFunction" @change="redisFunctionChange" placeholder="请选择模板类型" clearable
                         style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="dict in redisFunctionOptions"
                  :key="dict.dictValue"
                  :label="dict.dictLabel"
                  :value="dict.dictValue"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="1" class="el-col-12" v-show="redisFieldItem" id="redisFieldItem">
              <el-input v-model="form.redisValue" placeholder="请输入field" id="redisField" :disabled="detailViem"/>
            </el-form-item>
            <el-form-item label="2" class="el-col-7" v-show="redisKeyItem" id="redisKeyItem">
              <el-select v-model="form.redisKey" placeholder="请输入key" style="width: 100%" id="redisKey"
                         no-data-text="未选择变量分类" clearable :disabled="detailViem">
                <el-option-group :label="dataSourceName">
                  <el-option
                    v-for="data in variableFactorOptions"
                    :key="data.value"
                    :label="data.label"
                    :value="data.value"
                  />
                </el-option-group>
                <el-option-group :label="dataSourceTwoName">
                  <el-option
                    v-for="data in variableFactorTwoOptions"
                    :key="data.value"
                    :label="data.label"
                    :value="data.value"
                  />
                </el-option-group>
                <div v-for="dataAll in listDimension">
                  <el-option-group :label="dataAll.name" v-show="dimensionitem" size="medium">
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
            <el-form-item class="el-col-5" label-width="10px" v-show="redisKeyItem" id="effectFunctionId">
              <el-select v-model="form.redisSelfFunctionItem.effectFunction" placeholder="是否添加作用函数" clearable
                         style="width: 100%" @change="redisSelfFunctionChange" :disabled="detailViem">
                <el-option
                  v-for="data in redisSelfFunctionOptions"
                  :key="data.value"
                  :label="data.label"
                  :value="data.name"
                />
              </el-select>
            </el-form-item>
            <el-form-item class="el-col-1" label-width="2px" v-show="redisSelfFunctionShow">
              <el-input v-model="form.redisSelfFunctionItem.beginIndex" placeholder="start" :disabled="detailViem"/>
            </el-form-item>
            <el-form-item class="el-col-1" label-width="2px" v-show="redisSelfFunctionShow">
              <el-input v-model="form.redisSelfFunctionItem.lengthNum" placeholder="len" :disabled="detailViem"/>
            </el-form-item>


            <el-form-item label="元素去重" class="el-col-12" label-width="120px" v-show="redisElementDistinctItem">
              <el-select v-model="form.redisElementDistinct" placeholder="是否去重" clearable
                         style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="dict in redisElementDistinctOptions"
                  :key="dict.dictValue"
                  :label="dict.dictLabel"
                  :value="dict.dictValue"
                />
              </el-select>
            </el-form-item>
          </div>
          <!--        统计查询展示模块        -->
          <div v-show="countDiv">
            <el-form-item label="变量因子" class="el-col-7" prop="variableFactor">
              <el-select v-model="form.variableFactor" placeholder="请选择变量因子" clearable no-data-text="未选择变量分类"
                         style="width: 100%" :disabled="detailViem">
                <el-option-group :label="dataSourceName">
                  <el-option
                    v-for="data in variableFactorOptions"
                    :key="data.value"
                    :label="data.label"
                    :value="data.value"
                    :type="data.type"
                  />
                </el-option-group>
                <el-option-group :label="dataSourceTwoName" v-if="dataSourceTwoName!=='' && dataSourceTwoName!==null">
                  <el-option
                    v-for="data in variableFactorTwoOptions"
                    :key="data.value"
                    :label="data.label"
                    :value="data.value"
                    :type="data.type"
                  />
                </el-option-group>
                <div v-for="dataAll in listDimension">
                  <el-option-group :label="dataAll.name" v-show="dimensionitem" size="medium">
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
            <el-form-item class="el-col-5" label-width="10px" id="variableFactorFunctionId">
              <el-select v-model="form.statisticsSelfFunctionItem.effectFunction" @change="statisticsSelfFunctionChange"
                         placeholder="是否添加作用函数" clearable style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="data in redisSelfFunctionOptions"
                  :key="data.value"
                  :label="data.label"
                  :value="data.name"
                />
              </el-select>
            </el-form-item>
            <el-form-item class="el-col-1" label-width="2px" v-show="statisticsSelfFunctionShow">
              <el-input v-model="form.statisticsSelfFunctionItem.beginIndex" placeholder="start" :disabled="detailViem"/>
            </el-form-item>
            <el-form-item class="el-col-1" label-width="2px" v-show="statisticsSelfFunctionShow">
              <el-input v-model="form.statisticsSelfFunctionItem.lengthNum" placeholder="len" :disabled="detailViem"/>
            </el-form-item>
            <el-form-item label="统计计算模板" class="el-col-12" prop="statisticsCountModel">
              <el-select v-model="form.statisticsCountModel" placeholder="请选择计算模板" clearable style="width: 100%"
                         :disabled="detailViem">
                <el-option
                  v-for="data in statisticsCountModelOptions"
                  :key="data.dictValue"
                  :label="data.dictLabel"
                  :value="data.dictValue"
                />
              </el-select>
            </el-form-item>
            <div v-for="(itemGroup, index) in form.statisticsGroupItem" :key="index" class="el-col-24">
              <el-form-item label="统计分组" class="el-col-7">
                <el-select v-model="itemGroup.groupField" placeholder="请选择统计分组，该顺序即是分组顺序" clearable
                           no-data-text="未选择变量分类" style="width: 100%" :disabled="detailViem">
                  <el-option-group :label="dataSourceName">
                    <el-option
                      v-for="data in statisticsGroupOptions"
                      :key="data.value"
                      :label="data.name"
                      :value="data.value"
                    />
                  </el-option-group>
                  <el-option-group :label="dataSourceTwoName" v-if="dataSourceTwoName!=='' && dataSourceTwoName!==null">
                    <el-option
                      v-for="data in statisticsGroupTwoOptions"
                      :key="data.value"
                      :label="data.label"
                      :value="data.value"
                      :type="data.type"
                    />
                  </el-option-group>
                  <div v-for="dataAll in listDimension">
                    <el-option-group :label="dataAll.name" v-show="dimensionitem" size="medium">
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
              <el-form-item class="el-col-5" label-width="10px" :id="'groupFunctionId'+index">
                <el-select v-model="itemGroup.groupFunction" @change="groupFunctionChange($event, index)" placeholder="是否添加作用函数"
                           clearable
                           style="width: 100%" :disabled="detailViem">
                  <el-option
                    v-for="data in redisSelfFunctionOptions"
                    :key="data.value"
                    :label="data.label"
                    :value="data.name"
                  />
                </el-select>
              </el-form-item>
              <el-form-item class="el-col-1" label-width="2px" :id="'groupFunctionBeginIndexId'+index" v-show="false" >
                <el-input v-model="itemGroup.beginIndex" placeholder="start" :disabled="detailViem"/>
              </el-form-item>
              <el-form-item class="el-col-1" label-width="2px" :id="'groupFunctionLengthNumId'+index" v-show="false">
                <el-input v-model="itemGroup.lengthNum" placeholder="len" :disabled="detailViem"/>
              </el-form-item>
              <el-form-item label-width="10px" class="el-col-4">
                <span>
                  <el-button @click="addGroup" :disabled="detailViem"><i class="el-icon-plus"/></el-button>
                  <el-button @click="deleteGroup(itemGroup, index)" :disabled="detailViem"><i class="el-icon-minus"/></el-button>
                </span>
              </el-form-item>
            </div>

            <el-form-item label="统计周期" class="el-col-24">
              <el-select v-model="form.statisticsModel" @change="statisticsModelChange" placeholder="请选择统计周期模式" clearable style="width: 14%"
                         :disabled="detailViem">
                <el-option
                  v-for="dict in statisticsModelOptions"
                  :key="dict.dictValue"
                  :label="dict.dictLabel"
                  :value="dict.dictValue"
                />
              </el-select>
              <el-input v-model="form.globalVariableStartTime"  style="width: 16%" placeholder="开始时间" v-show="globalVariableModelShow" :disabled="detailViem"/>
              <el-input v-model="form.globalVariableStopTime"  style="width: 16%" placeholder="结束时间" v-show="globalVariableModelShow" :disabled="detailViem"/>
              <el-input-number v-model="form.statisticsNum" :min=0 :max="1000000" :step="1" v-show="statisticsModelShow" :disabled="detailViem"/>
              <el-select v-model="form.statisticsCycle" v-show="statisticsModelShow" placeholder="请选择统计周期" clearable
                         style="width: 14%"
                         :disabled="detailViem">
                <el-option
                  v-for="dict in statisticsCycleOptions"
                  :key="dict.dictValue"
                  :label="dict.dictLabel"
                  :value="dict.dictValue"
                />
              </el-select>
            </el-form-item>


          </div>
          <!--   普通查询和统计查询共用的   增加条件选项  -->
          <el-form-item class="el-col-24" v-show="normalAndCount">
            <div class="el-col-24">
              <el-button type="primary" @click="addConditionTable" :disabled="detailViem">增加条件选项</el-button>
              <div class="table">
                <el-table
                  :data="form.conditionTable"
                  tooltip-effect="dark"
                  border
                  stripe
                  style="width: 100%">
                  <el-table-column label="序号" type="index" width="60" align="center"/>
                  <el-table-column label="字段名称" align="center" width="750">
                    <template slot-scope="scope" style="float: right">
                      <el-select v-model="scope.row.field" placeholder="字段" clearable no-data-text="未选择数据源表"
                                 :disabled="detailViem" style="width:50%" @change="conditionFiledChange(scope)">
                        <el-option-group :label="dataSourceName">
                          <el-option
                            v-for="data in statisticsfieldOptions"
                            :key="data.value"
                            :label="data.name"
                            :value="data.value"
                            :type="data.type"
                          />
                        </el-option-group>
                        <el-option-group :label="dataSourceTwoName" v-if="dataSourceTwoName!=='' && dataSourceTwoName!==null">
                          <el-option
                            v-for="data in statisticsfieldTwoOptions"
                            :key="data.value"
                            :label="data.label"
                            :value="data.value"
                            :type="data.type"
                          />
                        </el-option-group>
                        <div v-for="dataAll in listDimension">
                          <el-option-group :label="dataAll.name" v-show="dimensionitem" size="medium">
                            <el-option
                              v-for="data in dataAll.dimensionStatisticsfieldOptions"
                              :key="data.value"
                              :label="data.label"
                              :value="data.value"
                              :type="data.type"
                            />
                          </el-option-group>
                        </div>
                      </el-select>
                      <el-select v-model="scope.row.functionType" @change="conditionTableFunctionChangeDetail(scope.row.functionType,scope.$index)" placeholder="是否添加作用函数" clearable
                                 style="width: 41%" :id="'conditionFunctionTypeId' +scope.$index" :disabled="detailViem">
                        <el-option
                          v-for="data in redisSelfFunctionOptions"
                          :key="data.value"
                          :label="data.label"
                          :value="data.name"
                        />
                      </el-select>
                      <el-input v-model="scope.row.beginIndex" style="width: 8%" :id="'conditionBeginIndexId' +scope.$index"
                                placeholder="start" :disabled="detailViem" v-show="false"/>
                      <el-input v-model="scope.row.lengthNum" style="width: 8%" :id="'conditionLengthNumId' +scope.$index"
                                placeholder="len" :disabled="detailViem" v-show="false"/>
                    </template>
                  </el-table-column>
                  <el-table-column label="类型" width="150" align="center">
                    <template slot-scope="scope">
                      <el-input v-model="scope.row.type" readonly="readonly" :disabled="detailViem"/>
                    </template>
                  </el-table-column>
                  <el-table-column label="运算符" width="150" align="center">
                    <template slot-scope="scope">
                      <el-select v-model="scope.row.symbol" placeholder="运算符" @change="symbolChange(scope)" clearable
                                 :disabled="detailViem">
                        <el-option
                          v-for="data in statisticsConditionOperatorOption"
                          :key="data.dictValue"
                          :label="data.dictLabel"
                          :value="data.dictValue"
                        />
                      </el-select>
                    </template>
                  </el-table-column>
                  <el-table-column label="值" align="center">
                    <template slot-scope="scope">
                      <el-input v-model="scope.row.value" :id="'ref' + scope.$index" :disabled="detailViem"/>
                    </template>
                  </el-table-column>
                  <el-table-column label="操作" align="center" width="200">
                    <template slot-scope="scope">
                      <el-button type="primary" @click="deleteConditionTable(scope)" :disabled="detailViem">删除</el-button>
                      <el-button type="primary" @click="joinContext(scope)" :disabled="detailViem">选择</el-button>
                    </template>
                  </el-table-column>
                </el-table>
              </div>
              <div>
                组合条件项：
                <el-button type="primary" size="mini" @click="leftKuoHao" :disabled="detailViem">(</el-button>
                <el-button type="primary" size="mini" @click="rightKuoHao" :disabled="detailViem">)</el-button>
                <el-button type="primary" size="mini" @click="andButton" :disabled="detailViem">and</el-button>
                <el-button type="primary" size="mini" @click="orButton" :disabled="detailViem">or</el-button>
                <el-button type="primary" size="mini" @click="deleteLast" :disabled="detailViem">删除</el-button>
                <el-button type="primary" size="mini" @click="clearContext" :disabled="detailViem">清空</el-button>
              </div>
              <div>
                <el-input v-model="form.statisticsConditions" readonly="readonly" type="textarea" :disabled="detailViem"/>
              </div>
            </div>
          </el-form-item>

          <!--        数据加工展示模块       -->
          <div v-show="processDiv">
            <el-form-item label="加工计算模板" class="el-col-12">
              <el-select v-model="form.processModel" placeholder="请选择计算模板" @change="querySelfSchema" clearable
                         style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="data in processModelOptions"
                  :key="data.value"
                  :label="data.name"
                  :value="data.value"
                  :selfFunctionNameCn="data.selfFunctionNameCn"
                />
              </el-select>
            </el-form-item>
            <!--输入字段展示-->
            <el-form-item class="el-col-24">
              <div class="el-col-23">
                <div class="table">
                  <el-table
                    :data="form.processTable"
                    tooltip-effect="dark"
                    border
                    stripe
                    style="width: 100%">
                    <el-table-column label="序号" type="index" width="60" align="center"/>
                    <el-table-column label="自定义函数输入字段" width="150" align="center" prop="schemaDefine"/>
                    <el-table-column label="字段字段" width="200" align="center" prop="fieldType" :formatter="fieldTypeFormat"/>
                    <el-table-column label="数据类型" align="center" width="150" prop="dataBaseType"/>
                    <el-table-column label="中文名" align="center" width="200" prop="schemaName"/>
                    <el-table-column label="参数对应字段" align="center" prop="selfDataSourceOutParams">
                      <template slot-scope="scope">
                        <span v-if="scope.row.fieldType === '01'">
                          <el-select v-model="scope.row.selfDataSourceOutParams" placeholder="请选择字段" clearable
                                     no-data-text="未选择变量分类" :disabled="detailViem" style="width:50%">
                            <el-option-group :label="dataSourceName">
                              <el-option
                                v-for="data in statisticsfieldOptions"
                                :key="data.value"
                                :label="data.name"
                                :value="data.value"
                                :type="data.type"
                              />
                            </el-option-group>
                            <el-option-group :label="dataSourceTwoName" v-if="dataSourceTwoName!=='' && dataSourceTwoName!==null">
                              <el-option
                                v-for="data in statisticsfieldTwoOptions"
                                :key="data.value"
                                :label="data.label"
                                :value="data.value"
                                :type="data.type"
                              />
                            </el-option-group>
                            <div v-for="dataAll in listDimension">
                              <el-option-group :label="dataAll.name" v-show="dimensionitem" size="medium">
                                <el-option
                                  v-for="data in dataAll.dimensionStatisticsfieldOptions"
                                  :key="data.value"
                                  :label="data.label"
                                  :value="data.value"
                                />
                              </el-option-group>
                            </div>
                          </el-select>
                        </span>
                        <span v-if="scope.row.fieldType === '02'">
                          <el-input v-model="scope.row.selfDataSourceOutParams" placeholder="请输入固定值" style="width:50%">
                          </el-input>
                        </span>
                        <el-select v-model="scope.row.functionType" @change="selfFunctionChnage(scope.row.functionType,scope.$index)"
                                   placeholder="是否添加作用函数" clearable :id="'selfFunctionTypeId' +scope.$index"
                                   style="width: 41%" :disabled="detailViem">
                          <el-option
                            v-for="data in redisSelfFunctionOptions"
                            :key="data.value"
                            :label="data.label"
                            :value="data.name"
                          />
                        </el-select>
                        <el-input v-model="scope.row.beginIndex" style="width: 8%" :id="'selffunctionBeginIndexId' +scope.$index"
                                  placeholder="start" :disabled="detailViem" v-show="false"/>
                        <el-input v-model="scope.row.lengthNum" style="width: 8%" :id="'selffunctionLengthNumId' +scope.$index"
                                  placeholder="len" :disabled="detailViem" v-show="false"/>
                      </template>
                    </el-table-column>
                  </el-table>
                </div>
              </div>
    <!--            <div class="el-col-16" style="padding-top: 20px">-->
    <!--              <div class="table">-->
    <!--                <el-table-->
    <!--                  :data="form.processOutputTable"-->
    <!--                  tooltip-effect="dark"-->
    <!--                  border-->
    <!--                  stripe-->
    <!--                  style="width: 100%">-->
    <!--                  <el-table-column label="序号" type="index" width="60" align="center"/>-->
    <!--                  <el-table-column label="自定义函数输出字段" align="center" prop="schemaDefine"/>-->
    <!--                  <el-table-column label="字段类型" align="center" prop="dataBaseType"/>-->
    <!--                </el-table>-->
    <!--              </div>-->
    <!--            </div>-->
            </el-form-item>
          </div>

          <!--        自定义查询展示模块-->
          <div v-show="userDefinedDiv">
            <el-form-item label="数据源表名" class="el-col-24" id="defineTableNameItem">
              <div v-for="data in defineTableNameItem">
                <el-button type="primary" size="mini" @click="tableButton" :disabled="detailViem">{{data.value}}
                </el-button>
              </div>
            </el-form-item>
            <el-form-item label="数据源表字段" class="el-col-24" id="defineSourceFieldItem">
              <span style="margin-right: 5px" v-for="(data,index) in variableFactorOptions">
                <el-button type="primary" size="mini" @click="tableButton"
                           :disabled="detailViem">{{data.value}}</el-button>
              </span>
            </el-form-item>
            <div v-for="dataAll in listDimension">
              <el-form-item :label="dataAll.name" class="el-col-24" v-show="defineDimensionFieldDiv">
              <span style="margin-right: 5px" v-for="data in dataAll.defineDimensionFieldItem">
                <el-button type="primary" size="mini" @click="tableButton"
                           :disabled="detailViem">{{data.label}}</el-button>
              </span>
              </el-form-item>
            </div>
            <el-form-item label="sql编辑区" class="el-col-24">
              <el-input v-model="form.userDefinedSql" type="textarea" style="width: 100%"
                        :autosize="{ minRows: 10, maxRows: 20}" placeholder="请编写正确有效的sql语句" :disabled="detailViem"/>
            </el-form-item>
          </div>

          <!--       决策引擎模块  -->
    <!--        <div v-show="decisionEngineDiv">-->
    <!--          <div>sss</div>-->
    <!--          &lt;!&ndash;    派生变量的模板类型      &ndash;&gt;-->
    <!--          -->
    <!--        </div>-->

          <!--        派生变量模块  -->
          <div v-show="deriveVariableDiv">
            <el-form-item label="模板类型" class="el-col-12">
              <el-select v-model="form.deriveVariableModelType" placeholder="请选择模板类型"
                         style="width: 100%" @change="deriveVariableModelTypeChange" :disabled="detailViem">
                <el-option
                  v-for="dict in deriveVariableModelTypeOptions"
                  :key="dict.dictValue"
                  :label="dict.dictLabel"
                  :value="dict.dictValue"
                />
              </el-select>
            </el-form-item>

    <!--          <el-form-item label="逻辑运算符" v-show="logicOperationModelShow" class="el-col-12">-->
    <!--            <el-select v-model="form.deriveProcessModel" placeholder="请选择逻辑运算符" @change="deriveProcessModelChange" clearable-->
    <!--                       style="width: 100%" :disabled="detailViem">-->
    <!--              <el-option-->
    <!--                v-for="dict in logicOperationOptions"-->
    <!--                :key="dict.dictValue"-->
    <!--                :label="dict.dictLabel"-->
    <!--                :value="dict.dictValue"-->
    <!--              />-->
    <!--            </el-select>-->
    <!--          </el-form-item>-->

            <el-form-item label="决策模板" v-show="deriveProcessModelShow" class="el-col-12">
              <el-select v-model="form.deriveProcessModel" placeholder="请选择决策模板" @change="deriveProcessModelChange" clearable
                         style="width: 100%" :disabled="detailViem">
                <el-option
                  v-for="data in decisionModelOptions"
                  :key="data.value"
                  :label="data.name"
                  :value="data.value"
                  :selfFunctionNameCn="data.selfFunctionNameCn"
                />
              </el-select>
            </el-form-item>

            <!--输入字段展示-->
            <el-form-item v-show="deriveEngineTableShow" class="el-col-24"  >
              <div class="el-col-23">
                <div class="table">
                  <el-table
                    :data="form.deriveEngineTable"
                    tooltip-effect="dark"
                    border
                    stripe
                    style="width: 100%">
                    <el-table-column label="序号" type="index" width="60" align="center"/>
                    <el-table-column label="自定义函数输入字段" width="150" align="center" prop="schemaDefine"/>
                    <el-table-column label="字段字段" width="200" align="center" prop="fieldType" :formatter="fieldTypeFormat"/>
                    <el-table-column label="数据类型" align="center" width="150" prop="dataBaseType"/>
                    <el-table-column label="中文名" align="center" width="200" prop="schemaName"/>
                    <el-table-column label="参数对应字段" align="center" prop="selfDataSourceOutParams">
                      <template slot-scope="scope">
                        <span v-if="scope.row.fieldType === '01'">
                          <treeselect v-model="scope.row.selfDataSourceOutParams" :options="allVariableOptions"
                                      :multiple="true"
                                      :appendToBody="true"
                                      :disableBranchNodes="true"
                                      :showCount="true"
                                      style="width:90%;margin-left: 5%"
                                      sortValueBy="INDEX"
                                      placeholder="请选择变量分类" :disabled="detailViem" no-data-text="未选择变量分类"/>
                        </span>
                        <span v-if="scope.row.fieldType === '02'">
                          <el-input v-model="scope.row.selfDataSourceOutParams" :disabled="detailViem" placeholder="请输入固定值" style="width:91%">
                          </el-input>
                        </span>
    <!--                      <span v-if="scope.row.dataBaseType === 'map'">-->
    <!--                                <el-button @click="addInputSource"><i class="el-icon-plus" :disabled="detailViem"/>-->
    <!--                                </el-button>-->
    <!--                                <el-button @click="removeInputSource(scope)"><i class="el-icon-minus"-->
    <!--                                                                                :disabled="detailViem"/></el-button>-->
    <!--                      </span>-->
    <!--                      <el-select v-model="scope.row.functionType" @change="selfFunctionChnage(scope.row.functionType,scope.$index)"-->
    <!--                                 placeholder="是否添加作用函数" clearable :id="'selfFunctionTypeId' +scope.$index"-->
    <!--                                 style="width: 41%" :disabled="detailViem">-->
    <!--                        <el-option-->
    <!--                          v-for="data in redisSelfFunctionOptions"-->
    <!--                          :key="data.value"-->
    <!--                          :label="data.label"-->
    <!--                          :value="data.name"-->
    <!--                        />-->
    <!--                      </el-select>-->
    <!--                      <el-input v-model="scope.row.beginIndex" style="width: 8%" :id="'selffunctionBeginIndexId' +scope.$index"-->
    <!--                                placeholder="start" :disabled="detailViem" v-show="false"/>-->
    <!--                      <el-input v-model="scope.row.lengthNum" style="width: 8%" :id="'selffunctionLengthNumId' +scope.$index"-->
    <!--                                placeholder="len" :disabled="detailViem" v-show="false"/>-->
                      </template>
                    </el-table-column>
                  </el-table>
                </div>
              </div>
            </el-form-item>

            <el-form-item label="变量编辑区" v-show="deriveVariableSqlShow" prop="deriveVariableSql" class="el-col-24">
              <div>
                <el-button type="primary" size="mini" @click="addWhere('(')" :disabled="detailViem">(</el-button>
                <el-button type="primary" size="mini" @click="addWhere(')')" :disabled="detailViem">)</el-button>
                <el-button type="primary" size="mini" @click="addWhere('+')" :disabled="detailViem">+</el-button>
                <el-button type="primary" size="mini" @click="addWhere('-')" :disabled="detailViem">-</el-button>
                <el-button type="primary" size="mini" @click="addWhere('*')" :disabled="detailViem">*</el-button>
                <el-button type="primary" size="mini" @click="addWhere('/')" :disabled="detailViem">/</el-button>
                <span v-show="logicOperationModelShow">
                <el-button type="primary" size="mini" @click="addWhere('=')" :disabled="detailViem">=</el-button>
                <el-button type="primary" size="mini" @click="addWhere('>')" :disabled="detailViem">></el-button>
                <el-button type="primary" size="mini" @click="addWhere('>=')" :disabled="detailViem">>=</el-button>
                <el-button type="primary" size="mini" @click="addWhere('<')" :disabled="detailViem"><</el-button>
                <el-button type="primary" size="mini" @click="addWhere('<=')" :disabled="detailViem"><=</el-button>
                <el-button type="primary" size="mini" @click="addWhere('<>')" :disabled="detailViem"><></el-button>
                <el-button type="primary" size="mini" @click="addWhere('and')" :disabled="detailViem">and</el-button>
                <el-button type="primary" size="mini" @click="addWhere('or')" :disabled="detailViem">or</el-button>
              </span>
              </div>
              <div @keyup.shift.50="deriveVariableSqlChange" @keyup.delete="deriveVariableSqlDelete">
                <el-input v-model="form.deriveVariableSql" type="textarea" style="width: 100%"
                          :autosize="{ minRows: 10, maxRows: 20}" id='ipt' :disabled="detailViem"/>
              </div>
              <ul id='tableList' v-clickoutside="handleClickOutSide" class="ulClass">
                <li v-for="(data,index) in baseVariableOptions" :key="index" :value="data.value"
                    :sourceInputParam="data.sourceInputParam" :dimensionInputParam="data.dimensionInputParam">{{data.label}}</li>
              </ul>
              <div>
                备注：1、输入@符号，会提示基础变量。
              </div>
            </el-form-item>

          </div>


          <!--        测试模块-->
          <div>
            <template>
              <div class="boxShadow">
                <el-dialog :title="title" :visible.sync="open" customClass="customWidth" :close-on-click-modal="false"
                           class="el-col-24" :show-close="false">
                  <el-tabs v-model="activeName" type="card" v-loading="testRunLoading">

                    <el-tab-pane label="测试数据" name="first">
                      <!--  数据源表                   -->
                      <el-form ref="form" :model="form" :rules="testRules" label-width="120px" class="el-col-24">
                        <div v-show="sourceTableValueItem">
                          <el-scrollbar>
                            <span style="font-size: 16px;font-weight: bold">{{dataSourceName}}</span>

                            <el-table :data="form.sourceTableValue" border ref="multipleTable" tooltip-effect="dark"
                                      style="width: 100%; margin-top: 10px"
                                      empty-text="未选择输入参数表">
                              <template v-for='(col,index) in sourceTableCol'>
                                <el-table-column :show-overflow-tooltip="true"
                                                 :prop="col.dataItem" align="center" :label="col.dataName" :key="index"
                                                 width="300px">
                                  <template scope="scope">
                                    <el-form-item label-width="0px" :prop="col.dataName.indexOf('主键') > 0 ? 'sourceTableValue.'+scope.$index+'.' + col.dataItem :
                                                     col.dataName.indexOf('水印') > 0 ? 'sourceTableValue.'+scope.$index+'.' + col.dataItem : ''"
                                                  :rules="col.dataName.indexOf('主键') > 0 ? testRules.primaryKey :
                                                     col.dataName.indexOf('水印') > 0 ? testRules.waterMark :[{ required: false }]">
                                      <el-input v-model="scope.row[col.dataItem]" placeholder="请输入内容" />
                                    </el-form-item>
                                  </template>
                                </el-table-column>
                              </template>
                              <el-table-column label="操作" width="200px" align="center"
                                               class-name="small-padding fixed-width">
                                <template slot-scope="scope">
                                  <el-button @click="addInputSource"><i class="el-icon-plus"/>
                                  </el-button>
                                  <el-button @click="removeInputSource(scope)"><i class="el-icon-minus"/></el-button>
                                </template>
                              </el-table-column>
                            </el-table>
                          </el-scrollbar>
                        </div>
                        <!--  数据维表                   -->
                        <div v-show="dimensionTableValueItem" style="margin-top: 10px">
                          <el-scrollbar>
                            <div v-for="(dataAll,indexList) in listResultDimension" style="margin-top: 10px">
                              <span style="font-size: 16px;font-weight: bold">{{dataAll.name}}</span>
                                <el-table :data="listResultDimension[indexList].dimensionTableValue" border ref="multipleTable"
                                          tooltip-effect="dark"
                                          style="width: 100%; margin-top: 10px"
                                          empty-text="未选择输入参数表">
                                  <template v-for='(col,index) in listResultDimension[indexList].dimensionTableCol'>
                                    <el-table-column :show-overflow-tooltip="true"
                                                     :prop="col.dataItem" align="center" :label="col.dataName" :key="index"
                                                     width="300px">
                                      <template scope="scope">
    <!--                                      <el-form-item label-width="0px" :prop="col.dataName.indexOf('主键') > 0 ? 'listResultDimension['+indexList+'].dimensionTableValue.'+scope.$index+'.' + col.dataItem : ''"-->
    <!--                                                    :rules="col.dataName.indexOf('主键') > 0 ? {required: true, message:'主键不能为空', trigger: 'blur'} : [{ required: false }]">-->
                                          <el-input v-model="scope.row[col.dataItem]" placeholder="请输入内容"
                                                  :disabled="detailViem"/>
    <!--                                      </el-form-item>-->
                                      </template>
                                    </el-table-column>
                                  </template>
                                  <el-table-column label="操作" width="200px" align="center"
                                                   class-name="small-padding fixed-width">
                                    <template slot-scope="scope">
                                      <el-button @click="addInputDimension(dataAll.name)"><i class="el-icon-plus"/>
                                      </el-button>
                                      <el-button @click="removeInputDimension(scope,dataAll.name)"><i
                                        class="el-icon-minus"/>
                                      </el-button>
                                    </template>
                                  </el-table-column>
                                </el-table>
                            </div>
                          </el-scrollbar>
                        </div>
                      </el-form>
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
                 <div style="margin-top: 30px;display: flex;justify-content: flex-end;">
                   <el-button type="primary" @click="confirmTest">开 始</el-button>
                   <el-button @click="cancelTest">关 闭</el-button>
                 </div>
                </el-dialog>
              </div>
            </template>
          </div>

          <el-form-item class="el-col-24">
            <div style="float: right">
              <el-button type="primary" @click="testRun" v-show="testButton" :disabled="testRunButton">调  试</el-button>
              <el-button type="primary" @click="submitForm" v-show="submitButton" :disabled="detailViem">确 定</el-button>
              <el-button @click="cancel">取 消</el-button>
            </div>
          </el-form-item>
        </div>
      </el-form>

    </div>

    <!--    版本详情-->
    <div v-show="detailDiv">
      <el-tabs v-model="versionTabName" type="card" >
        <el-tab-pane label="版本历史" name="versionFirst">
          <el-table v-loading="loading" :data="versionList" @row-dblclick="handleDetail">
            <el-table-column label="序号" width="55" align="center" type="index"/>
            <el-table-column label="变量中文名" align="center" prop="variableName"/>
            <el-table-column label="版本号" align="center" prop="versionNum"/>
            <el-table-column label="新增者"  align="center" prop="createBy"/>
            <el-table-column label="修改者"  align="center" prop="updateBy"/>
            <el-table-column label="新增时间" align="center" prop="createTime" >
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.createTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column label="修改时间" align="center" prop="updateTime" width="180">
              <template slot-scope="scope">
                <span>{{ parseTime(scope.row.updateTime) }}</span>
              </template>
            </el-table-column>
            <el-table-column
              label="操作"
              align="center"
              width="100"
              class-name="small-padding fixed-width"
            >
              <template slot-scope="scope">
                <el-button
                  size="mini"
                  type="text"
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
      <div style="float: right">
        <el-button  type="primary" @click="backButton" style="margin-right: 50px;margin-top: 30px;">返  回</el-button>
      </div>
    </div>
  </div>
</template>

<script>
  import {
    listCenter,
    listVersionCenter,
    getCenter,
    delCenter,
    addCenter,
    updateCenter,
    exportCenter,
    testVariable
  } from "@/api/variable/variableManager";
  import {listOperate} from "@/api/variable/variableOperate";
  import axios from "axios";
  import {getToken} from '@/utils/auth'
  import log from "../../monitor/job/log";
  import {listFunction} from "@/api/taskdevelop/function.js";
  import Treeselect from '@riophae/vue-treeselect';
  import '@riophae/vue-treeselect/dist/vue-treeselect.css';
  import {isLegitimateName} from "@/utils/validate.js";
  import {mapGetters} from "vuex";

  const clickoutside = {
    // 初始化指令
    bind(el, binding, vnode) {
      function documentHandler(e) {
        // 这里判断点击的元素是否是本身，是本身，则返回
        if (el.contains(e.target)) {
          return false;
        }
        // 判断指令中是否绑定了函数
        if (binding.expression) {
          // 如果绑定了函数 则调用那个函数，此处binding.value就是handleClose方法
          binding.value(e);
        }
      }

      // 给当前元素绑定个私有变量，方便在unbind中可以解除事件监听
      el.__vueClickOutside__ = documentHandler;
      document.addEventListener('click', documentHandler);
    },
    unbind(el, binding) {
      // 解除事件监听
      document.removeEventListener('click', el.__vueClickOutside__);
      delete el.__vueClickOutside__;
    },
  };

  export default {
    name: "Center",
    components: {Treeselect},
    data() {
      return {
        // 遮罩层
        loading: true,
        // 变量测试Loading
        testRunLoading: false,
        // 变量修改Loading
        updateLoading: false,
        // open
        open: false,
        asd: "",
        // 选中数组
        ids: [],
        names: undefined,
        // 非单个禁用
        single: true,
        // 非多个禁用
        multiple: true,
        // 总条数
        total: 0,
        // 变量管理中心表格数据
        centerList: [],
        versionList: [],
        operateList: [],
        // 弹出层标题
        title: "",
        // 变量类型 01基础变量 02 派生变量字典
        variableTypeOptions: [],
        // 模板类型 01 普通查询 02 统计查询 03 数据加工字典
        modelTypeOptions: [],
        // 派生变量模板类型
        deriveVariableModelTypeOptions: [],
        // 派生变量逻辑运算符
        logicOperationOptions: [],
        // 统计模式 01 滚动窗口  02 滑动窗口
        statisticsModelOptions: [],
        // 统计周期  01秒 02分 03小时 04天 05周 06月 07年
        statisticsCycleOptions: [],
        // 控制新增框的展示
        addDiv: false,
        // 详情的显示
        detailDiv: false,
        statisticsModelShow: true,
        globalVariableModelShow: false,
        // 调试按钮
        testRunButton: false,
        // 控制新增时是否展示
        layoutOne: true,
        // 数据维表的options
        dimensionDataOptions: [],
        // 所有的变量
        allVariableOptions: [],
        // 变量因子options
        variableFactorOptions: [],
        // 变量因子options 数据为数据源表(二)的字段
        variableFactorTwoOptions : [],
        dimensionDataProps: {
          label: 'label',
          children: 'dimensionSchemas'
        },
        //数据源表的options
        sourceDataOptions: [],
        sourceDataProps: {
          label: 'label',
          children: 'sourceSchemas'
        },
        // 加工计算模板option 来自自定义模板
        processModelOptions: [],
        // 决策计算模板option 来自自定义模板
        decisionModelOptions: [],
        // 统计计算模板options
        statisticsCountModelOptions: [],
        // 统计分组 数据为数据源表的字段
        statisticsGroupOptions: [],
        // 统计分组 数据为数据源表(二)的字段
        statisticsGroupTwoOptions : [],
        // 统计字段名称与类型
        statisticsfieldOptions: [],
        // 统计字段名称与类型 数据为数据源表(二)的字段
        statisticsfieldTwoOptions : [],

        // 统计运算符号
        statisticsConditionOperatorOption: [],
        // 变量分类option
        variableClassificationOptions: [],
        // redis方法
        redisFunctionOptions: [],
        // 基础变量选择options
        baseVariableOptions: [],
        // 所有的变量
        baseVariableOptions: [],

        defineTableNameItem: [],
        defineSourceFieldItem: [],
        variableArray: [],
        // 普通查询控制
        normalDiv: true,
        // 统计查询控制
        countDiv: false,
        // 数据加工控制
        processDiv: false,
        // 自定义查询控制
        userDefinedDiv: false,
        // 数据维表
        dimensionitem: false,
        // 控制rediskey
        redisKeyItem: false,
        redisElementDistinctItem: false,
        redisSelfFunctionShow: false,
        statisticsSelfFunctionShow: false,
        // 控制redisvalue
        redisFieldItem: false,
        // 控制数据维表字段
        defineDimensionFieldDiv: false,
        // 模板类型
        variableModelTypeItem: true,
        // 基础变量选项控制
        deriveVariableDiv: false,
        // 决策模板展示
        deriveProcessModelShow: false,
        // 逻辑运算符展示
        logicOperationModelShow: false,
        // 决策引擎模块
        decisionEngineDiv: false,

        // 数据源表头列
        sourceTableCol: [],
        // 测试时数据源表的主键和水印字段
        sourceTwoCol: [],
        // 控制数据维表列是否展示
        dimensionTableValueItem: false,
        sourceTableValueItem: false,
        //tabs标签页的初始页
        activeName: 'first',
        versionTabName: 'versionFirst',
        // 测试的结果集
        testResultData: [],
        // 测试的结果列
        testResultCol: [],
        // 控制测试按钮的展示
        testButton: true,
        // 确定按钮的 展示
        submitButton: true,
        // 变量编辑区
        deriveVariableSqlShow: true,
        deriveEngineTableShow: false,
        // 自定义查询点击的字段
        selfQueryTestItem: [],
        // 普通查询和统计查询的条件选项
        normalAndCount: true,
        // 控制标签是否可修改
        detailViem: false,
        // 有多少个数据维表
        listDimension: [],
        // 有多少个测试数据维表
        listResultDimension: [],
        // 数据源表的名字
        dataSourceName:"",
        // 数据源表(二)的名字
        dataSourceTwoName:"",
        // redis
        redisSelfFunctionOptions:[],
        // 元素去重
        redisElementDistinctOptions:[],
        // 控制选项修改时是否可修改
        UnAllowedUpdate:false,
        // 临时变量对象
        variableTmp:undefined,
        versionNumShow:false,

        fieldTypeOptions: [],
        // 查询参数
        queryParams: {
          pageNum: 1,
          pageSize: 10,
          variableName: undefined,
          variableNameEn: undefined,
          variableType: undefined,
          description: undefined,
          functionType: undefined,
          variableClassification: undefined,
        },
        testForm:{
          primaryKey:"",
        },
        // 表单参数
        form: {
          // 默认变量类型为基础变量
          variableType: "01",
          // 默认模板类型为：四则运算
          deriveVariableModelType: "01",
          // 默认模板类型为普通查询
          variableModelType: "01",
          statisticsConditions: "",
          userDefinedSql: "",
          variableFactor: "",
          deriveVariableSql: "",
          conditionTable: [],
          processTable: [],
          deriveEngineTable: [],
          processOutputTable: [],
          sourceTableValue: [],
          testSourceTableCol: [],
          statisticsGroupItem:[{
            groupField:"",
            groupFunction:"",
            beginIndex:undefined,
            lengthNum:undefined,
          }],
          redisSelfFunctionItem: {
            effectFunction:undefined,
            beginIndex:undefined,
            lengthNum:undefined,
          },
          statisticsSelfFunctionItem: {
            effectFunction:undefined,
            beginIndex:undefined,
            lengthNum:undefined,
          },
        },
        // 测试表单校验 'listResultDimension['+indexList+'].dimensionTableCol.'+index+'.' + col.dataItem : ''"
        testRules:{
          listResultDimension:{
            dimensionTableValue:{
              primaryKey:[
                {required: true, message: "主键不能为空", trigger: "blur"}
              ]
            }
          },
          primaryKey: [
            {required: true, message: "主键不能为空", trigger: "blur"}
          ],
          waterMark: [
            {required: true, message: "水印不能为空", trigger: "blur"}
          ],
          relationField: [
            {required: true, message: "关联字段不能为空", trigger: "blur"}
          ],
        },
        // 表单校验
        rules: {
          variableName: [
            {required: true, message: "变量中文名不能为空", trigger: "blur"}
          ],
          variableNameEn: [
            {required: true, message: "变量英文名不能为空", trigger: "blur"},
            {validator: isLegitimateName, message: "格式：只能包含英文字母、数字、下划线且首字母必须是英文字母", trigger: "blur"}
          ],
          variableType: [
            {required: true, message: "变量类型不能为空", trigger: "blur"}
          ],
          variableModelType: [
            {required: true, message: "模板类型不能为空", trigger: "blur"}
          ],
          variableFactor: [
            {required: true, message: "变量因子不能为空", trigger: "blur"}
          ],
          clusterName: [
            {required: true, message: "集群名不能为空", trigger: "blur"}
          ],
          statisticsCountModel: [
            {required: true, message: "计算模板不能为空", trigger: "blur"}
          ],
          variableClassification: [
            {required: true, message: "变量分类不能为空", trigger: "blur"}
          ],
          deriveVariableSql: [
            {required: true, message: "变量编辑区不能为空", trigger: "blur"}
          ]
        }
      };
    },
    created() {
      this.getList();
      this.getDicts("sys_redis_element_distinct").then(response => {
        this.redisElementDistinctOptions = response.data;
      });
      this.getDicts("variable_type").then(response => {
        this.variableTypeOptions = response.data;
      });
      this.getDicts("variable_model_type").then(response => {
        this.modelTypeOptions = response.data;
      });
      this.getDicts("sys_derive_variable_model_type").then(response => {
        this.deriveVariableModelTypeOptions = response.data;
      });
      this.getDicts("sys_logic_operation").then(response => {
        this.logicOperationOptions = response.data;
      });
      this.getDicts("sys_variable_statistics_model").then(response => {
        this.statisticsModelOptions = response.data;
      });
      this.getDicts("sys_variable_statistics_cycle").then(response => {
        this.statisticsCycleOptions = response.data;
      });
      this.getDicts("sys_statistics_condition_operator").then(response => {
        this.statisticsConditionOperatorOption = response.data;
      });
      this.getDicts("sys_variable_count_model").then(response => {
        this.statisticsCountModelOptions = response.data;
      });
      this.getDicts("sys_redis_function").then(response => {
        this.redisFunctionOptions = response.data;
      });
      this.getDicts("sys_function_field_type").then(response => {
        this.fieldTypeOptions = response.data;
      });
      this.getVariableClassification();
    },
    // 计算属性
    computed:{
      // 登录人的名
      ...mapGetters([
        'name'
      ]),
    },
    directives: {clickoutside},
    methods: {

      // 查询所有的变量
      getVariableCenter(value) {
        let that = this;
        that.allVariableOptions = [];
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
            that.allVariableOptions.push(
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
                  that.allVariableOptions[0].children[0].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '02') {
                  that.allVariableOptions[0].children[1].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '03') {
                  that.allVariableOptions[0].children[2].children.push(
                    {
                      id: resp.data.rows[i].variableNameEn,
                      label: resp.data.rows[i].variableName,
                      variableType: resp.data.rows[i].variableType,
                      testSourceTableCol: resp.data.rows[i].testSourceTableCol,
                      testDimensionTableCol: resp.data.rows[i].testDimensionTableCol,
                    }
                  );
                } else if (resp.data.rows[i].variableModelType === '04') {
                  that.allVariableOptions[0].children[3].children.push(
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
                that.allVariableOptions[1].children.push(
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
            console.log(that.allVariableOptions);
          }).catch(resp => {
            console.log('获取变量分类请求失败：' + resp.status + ',' + resp.statusText);
          });
        }
      }
      ,

      // 派生变量模板类型 01 四则运算  02 数据加工
      deriveVariableModelTypeChange(value) {
        if (value === "01") { // 四则运算
          this.deriveVariableSqlShow = true;
          this.deriveEngineTableShow = false;
          this.deriveProcessModelShow = false;
          this.logicOperationModelShow = false;
          this.rules.deriveVariableSql[0].required = true;
        } else if (value === "02") { // 计算引擎
          this.deriveVariableSqlShow = false;
          this.logicOperationModelShow = false;
          this.deriveEngineTableShow = true;
          this.deriveProcessModelShow = true;
          this.rules.deriveVariableSql[0].required = false;
        } else if (value === "03") { // 逻辑运算
          this.deriveVariableSqlShow = true;
          this.logicOperationModelShow = true;
          this.deriveEngineTableShow = false;
          this.deriveProcessModelShow = false;
          this.rules.deriveVariableSql[0].required = true;
        }
      },

      // 分组 作用函数切换
      statisticsModelChange(value) {

        if (value === "04" || value === "05" || value === "06") {
          this.statisticsModelShow = false; //
          this.globalVariableModelShow = false;
        }else if (value === "07") {
          this.statisticsModelShow = false;
          this.globalVariableModelShow = true;
        } else {
          this.statisticsModelShow = true;
          this.globalVariableModelShow = false;
        }
        console.log(value);
        console.log(this.statisticsModelShow);
        console.log(this.globalVariableModelShow);
      },

      // 自定义 作用函数切换
      selfFunctionChnage(value,index) {
        if (value === 'SUBSTRING') {
          document.querySelector('#selfFunctionTypeId'+index).parentElement.parentElement.setAttribute("style","width:25%");
          document.querySelector('#selffunctionBeginIndexId'+index).parentElement.setAttribute("style","width:8%");// beginIndex项是否显示
          document.querySelector('#selffunctionLengthNumId'+index).parentElement.setAttribute("style","width:8%");
        } else {
          document.querySelector('#selfFunctionTypeId'+index).parentElement.parentElement.setAttribute("style","width:41%");
          document.querySelector('#selffunctionBeginIndexId'+index).parentElement.setAttribute("style","display:none"); // beginIndex项是否显示
          document.querySelector('#selffunctionLengthNumId'+index).parentElement.setAttribute("style","display:none"); // lengthNum项是否显示
          this.form.processTable[index].beginIndex = "";
          this.form.processTable[index].lengthNum = "";
        }
      },
      // 条件租 作用函数切换
      conditionTableFunctionChangeDetail(value,index) {
        if (value === 'SUBSTRING') {
          document.querySelector('#conditionFunctionTypeId'+index).parentElement.parentElement.setAttribute("style","width:25%");
          document.querySelector('#conditionBeginIndexId'+index).parentElement.setAttribute("style","width:8%");// beginIndex项是否显示
          document.querySelector('#conditionLengthNumId'+index).parentElement.setAttribute("style","width:8%");
        } else {
          document.querySelector('#conditionFunctionTypeId'+index).parentElement.parentElement.setAttribute("style","width:41%");
          document.querySelector('#conditionBeginIndexId'+index).parentElement.setAttribute("style","display:none"); // beginIndex项是否显示
          document.querySelector('#conditionLengthNumId'+index).parentElement.setAttribute("style","display:none"); // lengthNum项是否显示
          this.form.conditionTable[index].beginIndex = "";
          this.form.conditionTable[index].lengthNum = "";
        }
      },
      // 分组 作用函数切换
      groupFunctionChange(scope,index) {
        if (scope === 'SUBSTRING') {
          document.querySelector('#groupFunctionId'+index).classList.remove("el-col-5");
          document.querySelector('#groupFunctionId'+index).classList.add("el-col-3");
          document.querySelector('#groupFunctionBeginIndexId'+index).removeAttribute("style");// beginIndex项是否显示
          document.querySelector('#groupFunctionLengthNumId'+index).removeAttribute("style"); // lengthNum项是否显示
        } else {
          document.querySelector('#groupFunctionId'+index).classList.remove("el-col-3");
          document.querySelector('#groupFunctionId'+index).classList.add("el-col-5");
          document.querySelector('#groupFunctionBeginIndexId'+index).setAttribute("style","display:none"); // beginIndex项是否显示
          document.querySelector('#groupFunctionLengthNumId'+index).setAttribute("style","display:none"); // lengthNum项是否显示
          this.form.statisticsGroupItem[index].beginIndex = "";
          this.form.statisticsGroupItem[index].lengthNum = "";
        }
      },

      // 变量因子 作用函数切换
      statisticsSelfFunctionChange(value) {
        if (value === 'SUBSTRING') {
          this.statisticsSelfFunctionShow = true;
          document.querySelector('#variableFactorFunctionId').classList.remove("el-col-5");
          document.querySelector('#variableFactorFunctionId').classList.add("el-col-3");
        } else {
          this.statisticsSelfFunctionShow = false;
          document.querySelector('#variableFactorFunctionId').classList.remove("el-col-3");
          document.querySelector('#variableFactorFunctionId').classList.add("el-col-5");
          this.form.statisticsSelfFunctionItem.beginIndex = "";
          this.form.statisticsSelfFunctionItem.lengthNum = "";
        }
      },
      // rediskey 作用函数切换
      redisSelfFunctionChange(value) {
        if (value === 'SUBSTRING') {
          this.redisSelfFunctionShow = true;
          document.querySelector('#effectFunctionId').classList.remove("el-col-5");
          document.querySelector('#effectFunctionId').classList.add("el-col-3");
        } else {
          this.redisSelfFunctionShow = false;
          document.querySelector('#effectFunctionId').classList.remove("el-col-3");
          document.querySelector('#effectFunctionId').classList.add("el-col-5");
          this.form.redisSelfFunctionItem.beginIndex = "";
          this.form.redisSelfFunctionItem.lengthNum = "";
        }

      },

      // 增加行
      addGroup() {
        this.form.statisticsGroupItem.push({
          groupField:"",
          groupFunction:"",
          beginIndex:"",
          lengthNum:"",
        });
      },
      // 删除行
      deleteGroup(item, index) {
        if (this.form.statisticsGroupItem && this.form.statisticsGroupItem.length === 1) {
          this.$alert("不能删除所有的列", {type: "error"});
          return false;
        }
        this.form.statisticsGroupItem.splice(index, 1);
      },

      // 连接器类型字典翻译
      fieldTypeFormat(row, column) {
        return this.selectDictLabel(this.fieldTypeOptions, row.fieldType);
      },

      // 查询作用函数
      getFunctionTypeOptions() {
        listFunction(this.queryParams).then(response => {
          for (let i = 0; i < response.rows.length; i++) {
            if (response.rows[i].functionType === "02" || response.rows[i].functionType === "00") {
              this.redisSelfFunctionOptions.push({
                value: response.rows[i].selfFunctionId,
                label: response.rows[i].selfFunctionName,
                name: response.rows[i].functionName,
              });
            }
          }
        });
      },

      // 选择添加选项中的字段，给后面的类型列赋值
      conditionFiledChange(scope){
        this.statisticsfieldOptions.map((data) =>{
          if (data.value === scope.row.field){

            this.form.conditionTable[scope.$index].type = data.type;
          }
        })
        for (let i = 0; i < this.listDimension.length; i++) {
          this.listDimension[i].dimensionStatisticsfieldOptions.map((data) =>{
            if (data.value === scope.row.field){
              this.form.conditionTable[scope.$index].type = data.type;
            }
          })
        }
      },

      // 新增输入行
      addInputSource() {
        this.form.sourceTableValue.push({})
      },
      // 移除输入行
      removeInputSource(scope) {
        if (this.form.sourceTableValue.length === 1) {
          this.$alert("不能全部删除！", {type: 'warning'});
          return;
        }
        this.form.sourceTableValue.splice(scope.$index, 1);
      },
      // 新增输入行
      addInputDimension(data) {
        for (let i = 0; i < this.listResultDimension.length; i++) {
          if (this.listResultDimension[i].name === data) {
            this.listResultDimension[i].dimensionTableValue.push({})
          }
        }
      },
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
      // 确认测试
      confirmTest() {
        // 先校验

        console.log(this.form)

        this.$refs["form"].validate((valid) => {
          if (valid) {
            for (let j = 0; j < this.form.sourceTableValue.length; j++) {
              for (let i = 0; i < this.sourceTableCol.length; i++) {
                if (JSON.stringify(this.form.sourceTableValue[j]).indexOf(this.sourceTableCol[i].dataItem) <= 0) {
                  let s = this.sourceTableCol[i].dataItem;
                  this.$set(this.form.sourceTableValue[j], s, '');
                }
              }
            }

            // this.open = false;
            if (this.form.variableType === '01') { // 基础变量
              if (this.form.variableModelType === '01') { // 普通查询
                // this.form.clusterName = this.form.clusterName.toString();
              } else if (this.form.variableModelType === '02') { // 统计查询
                this.form.statisticsConditionOption = JSON.stringify(this.form.conditionTable);
                // 变量因子的数据类型赋值
                // 若是数据源表的
                this.variableFactorOptions.map((data) => {
                  if (data.label === this.form.variableFactor) {
                    this.form.variableFactorType = data.type;
                  }
                });
                // 若数据维表的
                for (let i = 0; i < this.listDimension.length; i++) {
                  this.listDimension[i].dimensionRelationOptions.map((data) => {
                    if (data.key === this.form.variableFactor) {
                      this.form.variableFactorType = data.type;
                    }
                  })
                }
              } else if (this.form.variableModelType === '03') { // 数据加工
                let arr = [];
                for (let i = 0; i < this.form.processTable.length; i++) {
                  arr.push({
                    selfFuncParam: this.form.processTable[i].schemaDefine,
                    outParam: this.form.processTable[i].selfDataSourceOutParams,
                  })
                }
                this.form.processInputParams = JSON.stringify(arr);
              }
            } else if (this.form.variableType === '02') {
              // 保存计算引擎输入参数
              let arr = [];
              // [{"selfFuncParam":"source1","outParam":"1112"},{"selfFuncParam":"source2","outParam":"222"},{"selfFuncParam":"sourceMap","outParam":["ad","e"]}]
              // this.variableArray = [];
              console.log("00");
              console.log(this.form);
              console.log(this.form.deriveEngineTable);
              if (this.form.deriveVariableModelType === '02') {
                this.variableArray = [];
                for (let i = 0; i < this.form.deriveEngineTable.length; i++) {
                  arr.push({
                    selfFuncParam: this.form.deriveEngineTable[i].schemaDefine,
                    outParam: this.form.deriveEngineTable[i].selfDataSourceOutParams,
                    type: this.form.deriveEngineTable[i].dataBaseType,
                  });
                  if(this.form.deriveEngineTable[i].dataBaseType === "map"){
                    for (let j = 0; j < this.form.deriveEngineTable[i].selfDataSourceOutParams.length; j++) {
                      this.variableArray = this.variableArray.concat(this.form.deriveEngineTable[i].selfDataSourceOutParams[j]);
                    }
                  }
                }
              }
            }
            this.form.deriveBaseVariable = JSON.stringify(this.variableArray);
            // 主键赋值 sourceTwoCol 格式 ： id-主键
            this.form.sourceKey = this.sourceTwoCol[1];
            this.form.watermark = this.sourceTwoCol[0];
            this.form.testDimdata = this.listResultDimension;
            // 没出结果，一直等待
            let loading = this.$loading({
              lock: true,
              text: '正在调试，请耐心等待...',
              spinner: 'el-icon-loading',
              background: 'rgba(0, 0, 0, 0.2)'
            });
            testVariable(this.form).then(resp => {
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
          }
        })
      },
      // 取消测试
      cancelTest() {
        this.open = false;
        this.sourceTableCol = [];
        this.form.sourceTableValue = [];
        this.listResultDimension = [];
        this.dimensionTableValueItem = false;
        this.sourceTableValueItem = false;
        this.activeName = "first";
        this.testResultData = [];
        // this.selfQueryTestItem = [];
      },

      // 给 测试数据源展示 sourceTableCol  赋值
      setSourceTableCol() {
        this.listResultDimension = [];
        // 主键字段排第一位
        this.sourceTableCol.push({
          dataItem: this.sourceTwoCol[1],
          dataName: this.sourceTwoCol[1] + "-主键",
        })
        // 水印字段展示在最后一列
        this.sourceTableCol.push({
          dataItem: this.sourceTwoCol[0],
          dataName: this.sourceTwoCol[0] + "-水印",
        })
        if ("01" === this.form.variableModelType) { // 普通查询
          // redis key
          for (let i = 0; i < this.variableFactorOptions.length; i++) {
            if (this.variableFactorOptions[i].label === this.form.redisKey) {
              let split = this.form.redisKey.split(".");
              if (this.sourceTableCol[0].dataItem !== split[1] && this.sourceTableCol[1].dataItem !== split[1]) { // 去重
                this.sourceTableCol.push({
                  dataItem: this.form.redisKey,
                  dataName: this.form.redisKey,
                });
                break;
              }
            }
          }

          for (let i = 0; i < this.listDimension.length; i++) {
            for (let j = 0; j < this.listDimension[i].dimensionRelationOptions.length; j++) {
              if (this.listDimension[i].dimensionRelationOptions[j].label === this.form.redisKey) {
                this.listResultDimension.push({
                  name: this.listDimension[i].name,
                });

                let tmp = [];
                // 主键
                tmp.push({
                  dataItem: this.listDimension[i].primaryKeyValue,
                  dataName: this.listDimension[i].primaryKeyLabel,
                });
                if (this.listDimension[i].primaryKeyValue !== this.form.redisKey){ // 主键去重
                  tmp.push({
                    dataItem: this.form.redisKey,
                    dataName: this.form.redisKey,
                  });
                }

                Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp});
                let dmo = [];
                dmo.push({});
                Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                break;
              }
            }
          }

          // 条件选项  statisticsfieldOptions  dimensionStatisticsfieldOptions
          if (this.form.conditionTable !== null && this.form.conditionTable.length > 0) {
            for (let i = 0; i < this.form.conditionTable.length; i++) {
              for (let j = 0; j < this.statisticsfieldOptions.length; j++) {
                if (this.form.conditionTable[i].field === this.statisticsfieldOptions[j].value) {
                  let flag = true;
                  for (let k = 0; k < this.sourceTableCol.length; k++) {
                    let split1 = this.form.conditionTable[i].field.split(".");
                    if (this.sourceTableCol[k].dataItem === split1[1] || this.sourceTableCol[k].dataItem === this.form.conditionTable[i].field) {   // 去重
                      flag = false;
                    }
                  }
                  if (flag === true) {
                    this.sourceTableCol.push({
                      dataItem: this.form.conditionTable[i].field,
                      dataName: this.form.conditionTable[i].field,
                    });
                  }
                }
              }
            }
            for (let i = 0; i < this.form.conditionTable.length; i++) { // 条件
              for (let l = 0; l < this.listDimension.length; l++) {
                for (let j = 0; j < this.listDimension[l].dimensionStatisticsfieldOptions.length; j++) { // 全部的options
                  if (this.form.conditionTable[i].field === this.listDimension[l].dimensionStatisticsfieldOptions[j].value) {
                    // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                    let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                    let flag2 = true; // 都没有。最后才增加一个
                    let num = "";
                    group:for (let m = 0; m < this.listResultDimension.length; m++) { // 已经选择的
                      if (this.listResultDimension[m].name === this.listDimension[l].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                        flag2 = false;
                        for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {

                          let split1 = this.form.conditionTable[i].field.split(".");
                          if (this.listResultDimension[m].dimensionTableCol[k].dataItem === this.form.conditionTable[i].field) {   // 去重
                            flag1 = false;
                            break group;
                          }
                        }
                        num = m;
                        break group;
                      }
                    }
                    // 新增数据维表
                    if (flag2 === true) {
                      flag1 = false;
                      this.listResultDimension.push({
                        name: this.listDimension[l].name,
                      })
                      let tmp = [];
                      // 主键
                      tmp.push({
                        dataItem: this.listDimension[l].primaryKeyValue,
                        dataName: this.listDimension[l].primaryKeyLabel,
                      });
                      if (this.listDimension[l].primaryKeyValue !== this.form.conditionTable[i].field){ // 新增的与主键是否相同
                        tmp.push({
                          dataItem: this.form.conditionTable[i].field,
                          dataName: this.form.conditionTable[i].field,
                        })
                      }

                      let dmo = [];
                      dmo.push({});
                      if (this.listResultDimension.length === 0) {
                        Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                      } else {
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                      }
                    }
                    // 有数据维表 增加一个列 dimensionTableCol
                    if (flag1 === true) {
                      this.listResultDimension[num].dimensionTableCol.push({
                        dataItem: this.form.conditionTable[i].field,
                        dataName: this.form.conditionTable[i].field,
                      })
                    }
                  }
                }
              }

            }
          }
        } else if ("02" === this.form.variableModelType) { // 统计查询
          // 变量因子
          for (let i = 0; i < this.variableFactorOptions.length; i++) {
            if (this.variableFactorOptions[i].label === this.form.variableFactor) {
              let split = this.form.variableFactor.split(".");
              if (this.sourceTableCol[0].dataItem !== split[1] && this.sourceTableCol[1].dataItem !== split[1]) { // 去重
                this.sourceTableCol.push({
                  dataItem: this.form.variableFactor,
                  dataName: this.form.variableFactor,
                });
                break;
              }
            }
          }

          for (let i = 0; i < this.listDimension.length; i++) {
            for (let j = 0; j < this.listDimension[i].dimensionRelationOptions.length; j++) {
              if (this.listDimension[i].dimensionRelationOptions[j].label === this.form.variableFactor) {
                this.listResultDimension.push({
                  name: this.listDimension[i].name,
                });

                let tmp = [];
                // 主键
                tmp.push({
                  dataItem: this.listDimension[i].primaryKeyValue,
                  dataName: this.listDimension[i].primaryKeyLabel,
                });
                if (this.listDimension[i].primaryKeyValue !== this.form.variableFactor){ // 主键去重
                  tmp.push({
                    dataItem: this.form.variableFactor,
                    dataName: this.form.variableFactor,
                  });
                }

                Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp});
                let dmo = [];
                dmo.push({});
                Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                break;
              }
            }
          }
          // 统计分组
          if (this.form.statisticsGroupItem !== null && this.form.statisticsGroupItem.length > 0) {
            for (let i = 0; i < this.statisticsGroupOptions.length; i++) {
              for (let j = 0; j < this.form.statisticsGroupItem.length; j++) {
                if (this.statisticsGroupOptions[i].name === this.form.statisticsGroupItem[j].groupField) {
                  let flag = true;
                  for (let k = 0; k < this.sourceTableCol.length; k++) {
                    let split1 = this.form.statisticsGroupItem[j].groupField.split(".");
                    if (this.sourceTableCol[k].dataItem === split1[1] || this.sourceTableCol[k].dataItem === this.form.statisticsGroupItem[j].groupField) {   // 去重
                      flag = false;
                    }
                  }
                  if (flag === true) {
                    this.sourceTableCol.push({
                      dataItem: this.form.statisticsGroupItem[j].groupField,
                      dataName: this.form.statisticsGroupItem[j].groupField,
                    });
                  }

                }
              }
            }
            for (let g = 0; g < this.listDimension.length; g++) {
              for (let j = 0; j < this.form.statisticsGroupItem.length; j++) {
                for (let i = 0; i < this.listDimension[g].dimensionRelationOptions.length; i++) {
                  if (this.listDimension[g].dimensionRelationOptions[i].label === this.form.statisticsGroupItem[j].groupField) { // 字段相等 才判断
                    // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                    let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                    let flag2 = true; // 都没有。最后才增加一个
                    let num = "";
                    group:for (let m = 0; m < this.listResultDimension.length; m++) {
                      if (this.listResultDimension[m].name === this.listDimension[g].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                        flag2 = false;
                        for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {
                          if (this.listResultDimension[m].dimensionTableCol[k].dataItem === this.form.statisticsGroupItem[j].groupField) {   // 去重

                            flag1 = false;
                            break group;
                          }
                        }
                        num = m;
                        break group;
                      }
                    }
                    // 新增数据维表
                    if (flag2 === true) {
                      flag1 = false;
                      this.listResultDimension.push({
                        name: this.listDimension[g].name,
                      })
                      let tmp = [];
                      let strings = this.listDimension[g].primaryKeyValue.split(".");
                      // 主键
                      tmp.push({
                        dataItem: strings[1],
                        dataName: this.listDimension[g].primaryKeyLabel,
                      });

                      if(this.listDimension[g].primaryKeyValue !== this.form.statisticsGroupItem[j].groupField){ // 排除该字段与主键相同
                        tmp.push({
                          dataItem: this.form.statisticsGroupItem[j].groupField,
                          dataName: this.form.statisticsGroupItem[j].groupField,
                        })
                      }

                      let dmo = [];
                      dmo.push({});
                      if (this.listResultDimension.length === 0) {
                        Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                      } else {
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                      }
                    }
                    // 有数据维表 增加一个列
                    if (flag1 === true) {
                      if(this.listDimension[g].primaryKeyValue !== this.form.statisticsGroupItem[j].groupField) { // 排除该字段与主键相同
                        this.listResultDimension[num].dimensionTableCol.push({
                          dataItem: this.form.statisticsGroupItem[j].groupField,
                          dataName: this.form.statisticsGroupItem[j].groupField,
                        })
                      }
                    }
                  }
                }
              }
            }
          }
          // 条件选项  statisticsfieldOptions  dimensionStatisticsfieldOptions
          if (this.form.conditionTable !== null && this.form.conditionTable.length > 0) {
            for (let i = 0; i < this.form.conditionTable.length; i++) {
              for (let j = 0; j < this.statisticsfieldOptions.length; j++) {
                if (this.form.conditionTable[i].field === this.statisticsfieldOptions[j].value) {
                  let flag = true;
                  for (let k = 0; k < this.sourceTableCol.length; k++) {
                    let split1 = this.form.conditionTable[i].field.split(".");
                    if (this.sourceTableCol[k].dataItem === split1[1] || this.sourceTableCol[k].dataItem === this.form.conditionTable[i].field) {   // 去重
                      flag = false;
                    }
                  }
                  if (flag === true) {
                    this.sourceTableCol.push({
                      dataItem: this.form.conditionTable[i].field,
                      dataName: this.form.conditionTable[i].field,
                    });
                  }
                }
              }
            }
            for (let i = 0; i < this.form.conditionTable.length; i++) { // 条件
              for (let l = 0; l < this.listDimension.length; l++) {
                for (let j = 0; j < this.listDimension[l].dimensionStatisticsfieldOptions.length; j++) { // 全部的options
                  if (this.form.conditionTable[i].field === this.listDimension[l].dimensionStatisticsfieldOptions[j].value) {
                    // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                    let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                    let flag2 = true; // 都没有。最后才增加一个
                    let num = "";
                    group:for (let m = 0; m < this.listResultDimension.length; m++) { // 已经选择的
                      if (this.listResultDimension[m].name === this.listDimension[l].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                        flag2 = false;
                        for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {

                          let split1 = this.form.conditionTable[i].field.split(".");
                          if (this.listResultDimension[m].dimensionTableCol[k].dataItem === this.form.conditionTable[i].field) {   // 去重
                            flag1 = false;
                            break group;
                          }
                        }
                        num = m;
                        break group;
                      }
                    }
                    // 新增数据维表
                    if (flag2 === true) {
                      flag1 = false;
                      this.listResultDimension.push({
                        name: this.listDimension[l].name,
                      })
                      let tmp = [];
                      // 主键
                      tmp.push({
                        dataItem: this.listDimension[l].primaryKeyValue,
                        dataName: this.listDimension[l].primaryKeyLabel,
                      });
                      if (this.listDimension[l].primaryKeyValue !== this.form.conditionTable[i].field){ // 新增的与主键是否相同
                        tmp.push({
                          dataItem: this.form.conditionTable[i].field,
                          dataName: this.form.conditionTable[i].field,
                        })
                      }

                      let dmo = [];
                      dmo.push({});
                      if (this.listResultDimension.length === 0) {
                        Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                      } else {
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                      }
                    }
                    // 有数据维表 增加一个列 dimensionTableCol
                    if (flag1 === true) {
                      this.listResultDimension[num].dimensionTableCol.push({
                        dataItem: this.form.conditionTable[i].field,
                        dataName: this.form.conditionTable[i].field,
                      })
                    }
                  }
                }
              }

            }
          }
        } else if ("03" === this.form.variableModelType) { // 数据加工
          // 主键字段排第一位
          // 函数的参数的值
          if (this.form.processTable !== null && this.form.processTable.length > 0) {
            // 数据源表的
            for (let i = 0; i < this.statisticsGroupOptions.length; i++) {
              for (let j = 0; j < this.form.processTable.length; j++) {
                if (this.statisticsGroupOptions[i].name === this.form.processTable[j].selfDataSourceOutParams) {
                  let flag = true;
                  for (let k = 0; k < this.sourceTableCol.length; k++) {
                    let split1 = this.form.processTable[j].selfDataSourceOutParams.split(".");
                    if (this.sourceTableCol[k].dataItem === split1[1]) {   // 去重
                      flag = false;
                    }
                  }
                  if (flag === true) {
                    this.sourceTableCol.push({
                      dataItem: this.form.processTable[j].selfDataSourceOutParams,
                      dataName: this.form.processTable[j].selfDataSourceOutParams,
                    });
                  }

                }
              }
            }
            // 数据维表的
            for (let l = 0; l < this.listDimension.length; l++) {
              for (let i = 0; i < this.listDimension[l].dimensionRelationOptions.length; i++) {
                for (let j = 0; j < this.form.processTable.length; j++) {
                  if (this.listDimension[l].dimensionRelationOptions[i].label === this.form.processTable[j].selfDataSourceOutParams) {
                    // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                    let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                    let flag2 = true; // 都没有。最后才增加一个
                    let num = "";
                    group:for (let m = 0; m < this.listResultDimension.length; m++) { // 已经选择的
                      if (this.listResultDimension[m].name === this.listDimension[l].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                        flag2 = false;
                        for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {
                          if (this.listResultDimension[m].dimensionTableCol[k].dataItem === this.form.conditionTable[i].field) {   // 去重
                            flag1 = false;
                            break group;
                          }
                        }
                        num = m;
                        break group;
                      }
                    }
                    // 新增数据维表
                    if (flag2 === true) {
                      flag1 = false;
                      this.listResultDimension.push({
                        name: this.listDimension[l].name,
                      })
                      let tmp = [];
                      // 主键
                      tmp.push({
                        dataItem: this.listDimension[l].primaryKeyValue,
                        dataName: this.listDimension[l].primaryKeyLabel,
                      });
                      if (this.listDimension[l].primaryKeyValue !== this.form.processTable[j].selfDataSourceOutParams) { // 排除该字段与主键相同
                        tmp.push({
                          dataItem: this.form.processTable[j].selfDataSourceOutParams,
                          dataName: this.form.processTable[j].selfDataSourceOutParams,
                        });
                      }
                      let dmo = [];
                      dmo.push({});
                      if (this.listResultDimension.length === 0) {

                        Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                      } else {
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                      }
                    }
                    // 有数据维表 增加一个列 dimensionTableCol
                    if (flag1 === true) {
                      if (this.listDimension[l].primaryKeyValue !== this.form.processTable[j].selfDataSourceOutParams) { // 排除该字段与主键相同
                        this.listResultDimension[num].dimensionTableCol.push({
                          dataItem: this.form.processTable[j].selfDataSourceOutParams,
                          dataName: this.form.processTable[j].selfDataSourceOutParams,
                        })
                      }
                    }
                  }
                }
              }
            }

          }
        } else if ("04" === this.form.variableModelType) { // 自定义函数
          // 主键字段排第一位
          for (let i = 0; i < this.variableFactorOptions.length; i++) {
            let name = this.variableFactorOptions[i].value;  // jdbc.id
            if (this.form.userDefinedSql.indexOf(name) > 0){ // 包含
              let split = name.split(".");
              let flag = true;
              for (let j = 0; j <  this.sourceTableCol.length; j++) {
                if (split[1] === this.sourceTableCol[j].dataItem){ //相等 - 去重
                  flag = false;
                }
              }
              if (flag === true){
                this.sourceTableCol.push({
                  dataItem: name,
                  dataName: name,
                })
              }
            }
          }
          for (let i = 0; i < this.listDimension.length; i++) { // 循环大维表
            for (let j = 0; j < this.listDimension[i].dimensionRelationOptions.length; j++) { // 循环维表字段
              if (this.form.userDefinedSql.indexOf(this.listDimension[i].dimensionRelationOptions[j].label) > 0){ // 如果选中了该字段
                let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                let flag2 = true; // 都没有。最后才增加一个
                let num = "";
                group:for (let k = 0; k < this.listResultDimension.length; k++) { // 已经添加了的
                  if (this.listDimension[i].name === this.listResultDimension[k].name) { // 如果表名相等，只增加字段
                    flag2 = false;
                    for (let l = 0; l < this.listResultDimension[k].dimensionTableCol.length; l++) {
                      if (this.listResultDimension[k].dimensionTableCol[l].dataItem === this.listDimension[i].dimensionRelationOptions[j].label){ //已经添加该字段了，去重
                        flag1 = false;
                      }
                    }
                    num = k;
                    break group;
                  }
                }
                if (flag2 === true) {
                  flag1 = false;
                  this.listResultDimension.push({
                    name: this.listDimension[i].name,
                  })
                  let tmp = [];
                  // 主键
                  tmp.push({
                    dataItem: this.listDimension[i].primaryKeyValue,
                    dataName: this.listDimension[i].primaryKeyLabel,
                  });
                  if (this.listDimension[i].primaryKeyValue !== this.listDimension[i].dimensionRelationOptions[j].label) { // 排除该字段与主键相同
                    tmp.push({
                      dataItem: this.listDimension[i].dimensionRelationOptions[j].label,
                      dataName: this.listDimension[i].dimensionRelationOptions[j].label,
                    });
                  }
                  let dmo = [];
                  dmo.push({});
                  if (this.listResultDimension.length === 0) {
                    Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                    Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                  } else {
                    Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                    Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                  }
                }
                // 有数据维表 增加一个列 dimensionTableCol
                if (flag1 === true) {
                  if (this.listDimension[i].primaryKeyValue !== this.listDimension[i].dimensionRelationOptions[j].label) { // 排除该字段与主键相同
                    this.listResultDimension[num].dimensionTableCol.push({
                      dataItem: this.listDimension[i].dimensionRelationOptions[j].label,
                      dataName: this.listDimension[i].dimensionRelationOptions[j].label,
                    })
                  }
                }
              }
            }

          }

          // for (let i = 0; i < this.selfQueryTestItem.length; i++) {   // 循环已点击选择的字段
          //   // 数据源维表的
          //   for (let l = 0; l < this.listDimension.length; l++) {
          //     for (let j = 0; j < this.listDimension[l].dimensionRelationOptions.length; j++) { // 循环数据源表字段
          //       if (this.listDimension[l].dimensionRelationOptions[j].label === this.selfQueryTestItem[i]) { // 判断该字段是否属于数据原表
          //         // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
          //         let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
          //         let flag2 = true; // 都没有。最后才增加一个
          //         let num = "";
          //         group:for (let m = 0; m < this.listResultDimension.length; m++) { // 已经选择的
          //           if (this.listResultDimension[m].name === this.listDimension[l].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
          //             flag2 = false;
          //             // for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {
          //               // let split1 = this.selfQueryTestItem[i].split(".");
          //               // if (this.listResultDimension[m].dimensionTableCol[k].dataItem === split1[1]) {   // 去重
          //               //   flag1 = false;
          //               //   break group;
          //               // }
          //             // }
          //             num = m;
          //             break group;
          //           }
          //         }
          //         // 新增数据维表
          //         if (flag2 === true) {
          //           flag1 = false;
          //           this.listResultDimension.push({
          //             name: this.listDimension[l].name,
          //           })
          //           let tmp = [];
          //           // 主键
          //           tmp.push({
          //             dataItem: this.listDimension[l].primaryKeyValue,
          //             dataName: this.listDimension[l].primaryKeyLabel,
          //           });
          //           if (this.listDimension[l].primaryKeyValue !== this.selfQueryTestItem[i]) { // 排除该字段与主键相同
          //             tmp.push({
          //               dataItem: this.selfQueryTestItem[i],
          //               dataName: this.selfQueryTestItem[i],
          //             });
          //           }
          //           let dmo = [];
          //           dmo.push({});
          //           if (this.listResultDimension.length === 0) {
          //
          //             Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
          //             Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
          //           } else {
          //             Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
          //             Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
          //           }
          //         }
          //         // 有数据维表 增加一个列 dimensionTableCol
          //         if (flag1 === true) {
          //           if (this.listDimension[l].primaryKeyValue !== this.selfQueryTestItem[i]) { // 排除该字段与主键相同
          //             this.listResultDimension[num].dimensionTableCol.push({
          //               dataItem: this.selfQueryTestItem[i],
          //               dataName: this.selfQueryTestItem[i],
          //             })
          //           }
          //         }
          //       }
          //     }
          //   }
          //
          // }

        }
        // // 添加关联字段
        // this.relationField();
      },

      // 关联字段
      relationField(callback) {

        if (this.listResultDimension.length > 0) {
          // 解决测试时，关联的数据维没有选择时，则不需要在数据源表中有关联字段
          let tableName = [];
          for (let i = 0; i < this.listResultDimension.length; i++) {
            let split = this.listResultDimension[i].name.split(":")
            console.log(split);
            tableName = tableName.concat(split[1]);
          }

          this.variableClassificationOptions.map((data) => {
            if (data.value === this.form.variableClassification) {
              let parse = JSON.parse(data.dimensionRelation);
              // 字段中没有关联字段时，需增加
              let tmpArr = [];
              for (let j = 0; j < parse.length; j++) {
                let flag = true;
                for (let i = 0; i < this.sourceTableCol.length; i++) {
                  let tmp = this.sourceTableCol[i].dataItem;
                  if (parse[j].relation){
                    flag = true;
                  }
                  if (flag !== true && tmp === parse[j].sourceDabField) {
                    flag = false;
                    if (this.sourceTableCol[i].dataName.indexOf("主键") <= 0) { // 主键不修改
                      this.sourceTableCol[i].dataName = parse[j].sourceDabField + "-" + parse[j].dimensionName +
                        "关联字段";
                    }
                  }
                }
                if (flag === true) {
                  if(tableName.indexOf(parse[j].dimensionName) > -1){
                    tmpArr.push({
                      dataItem: parse[j].sourceDabField,
                      dataName: parse[j].sourceDabField + "-" + parse[j].dimensionName + "关联字段",
                    })

                  }
                }
                // 如果有关联es表，那么分别提取数据源表与之关联的字段
                // else if(flag === true){
                //   for (let k = 0; k < parse[j].relation.length; k++) {
                //     // 拼接临时数据
                //     let tmpData = this.sourceTableCol.concat(tmpArr);
                //     var flag2 = true;
                //     for(let m = 0; m<tmpData.length;m++){
                //       let tmp = tmpData[m].dataItem;
                //       if (tmp === parse[j].relation[k].sourceDabField) {
                //         flag2 = false;
                //         if (tmpData[m].dataName.indexOf("主键") <= 0) { // 主键不修改
                //           tmpData[m].dataName = parse[j].relation[k].sourceDabField + "-" + parse[j]
                //             .dimensionName + "." + parse[j].relation[k].dimensionDabField + "关联字段";
                //         }
                //       }
                //     }
                //     if(flag2){
                //       tmpArr.push({
                //         dataItem: parse[j].relation[k].sourceDabField,
                //         dataName: parse[j].relation[k].sourceDabField + "-" + parse[j].dimensionName + "." + parse[j].relation[k].dimensionDabField + "关联字段",
                //       });
                //     }
                //   }
                // }
              }
              this.sourceTableCol = this.sourceTableCol.concat(tmpArr);
            }
          });
        }
        callback();
      },

      // 获取基础变量对应的测试数据
      getBaseVariableTestCol(value) {
        setTimeout(_ => {
          let that = this;
          let tmp = "";
          for (let i = 0; i < value.length; i++) {
            tmp = tmp + value[i] + ","
          }
          // axios请求
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'post',
            url: baseUrl + '/variable/manager/testCol',
            headers: {'Authorization': 'Bearer ' + getToken()},
            responseType: 'json',
            params: {
              names: tmp.substr(0, tmp.length - 1),
            }
          }).then(function (resp) {
            for (let i = 0; i < resp.data.rows.length; i++) {
              // 数据源表的
              let parse = JSON.parse(resp.data.rows[i].testSourceTableCol);
              if (parse != null) {
                for (let j = 0; j < parse.length; j++) {
                  // 去重
                  let flag = true;
                  for (let k = 0; k < that.sourceTableCol.length; k++) {
                    let tmp = parse[j];
                    if (parse[j].indexOf(".") > 0) {
                      let split = tmp.split(".");
                      tmp = split[1];
                    } else if (parse[j].indexOf("-") > 0) {
                      let split = tmp.split("-");
                      tmp = split[0];
                    }
                    let tmp2 = that.sourceTableCol[k].dataName;
                    if (that.sourceTableCol[k].dataName .indexOf(".") > 0) {
                      let split = tmp2.split(".");
                      tmp2 = split[1];
                    } else if (that.sourceTableCol[k].dataName .indexOf("-") > 0) {
                      let split = tmp2.split("-");
                      tmp2 = split[0];
                    }


                    if (tmp2 === tmp) {
                      flag = false;
                    }
                  }
                  if (flag === true) {
                    that.sourceTableCol = that.sourceTableCol.concat({
                      dataItem: parse[j],
                      dataName: parse[j],
                    })
                  }
                }
              }
              // 数据维表的
              // [{"name":"数据维表:hbase","col":["列族1.列名1","列族1.列名11"]},{"name":"数据维表:jdbc","col":["id","iphone"]}]
              // 把这个赋值给 listResultDimension
              let parse2 = JSON.parse(resp.data.rows[i].testDimensionTableCol);
              for (let j = 0; j < parse2.length; j++) {
                first:for (let l = 0; l < parse2[j].col.length; l++) { // iphone

                  // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                  let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                  let flag2 = true; // 都没有。最后才增加一个
                  let num = "";
                  group:for (let m = 0; m < that.listResultDimension.length; m++) { // 已经添加的
                    if (that.listResultDimension[m].name === parse2[j].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                      flag2 = false;
                      for (let k = 0; k < that.listResultDimension[m].dimensionTableCol.length; k++) { // 循环同维表里的字段 id  iphone
                        if (that.listResultDimension[m].dimensionTableCol[k].dataItem === parse2[j].col[l]) {   // 去重
                          flag1 = false;
                          break group;
                        }
                        num = m;
                      }
                    }
                  }
                  // 新增数据维表
                  if (flag2 === true) {
                    flag1 = false;
                    that.listResultDimension.push({
                      name: parse2[j].name,
                    });
                    let t = [];
                    for (let k = 0; k < parse2[j].col.length; k++) {
                      t.push({
                        dataItem: parse2[j].col[k],
                        dataName: parse2[j].col[k],
                      })
                    }
                    let dmo = [];
                    dmo.push({});
                    if (that.listResultDimension.length === 0) {
                      Object.assign(that.listResultDimension[0], {dimensionTableCol: t}); // 不是第一个时
                      Object.assign(that.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                    } else {
                      Object.assign(that.listResultDimension[that.listResultDimension.length - 1], {dimensionTableCol: t}); // 不是第一个时
                      Object.assign(that.listResultDimension[that.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                    }
                    break first;  // 新增的表名就把所有的字段都新增
                  }

                  // 有数据维表 增加一个列 dimensionTableCol
                  if (flag1 === true) {
                    that.listResultDimension[num].dimensionTableCol.push({
                      dataItem: parse2[j].col[l],
                      dataName: parse2[j].col[l],
                    })
                  }
                }
              }
            }
          }).catch(resp => {
            console.log('获取基础变量对应的测试数据!' + resp);
          });
        }, 300);
      },

      // 点击测试
      testRun() {
        this.$refs["form"].validate(valid => {
          if (valid) {
            this.testRunLoading = true;
            this.open = true;
            this.title = "变量测试";
            // 给测试的参数赋值
            if (this.form.variableType === '01') { //基础变量
              this.setSourceTableCol();
            } else if (this.form.variableType === '02') { // 派生变量
              // 主键字段排第一位
              this.sourceTableCol.push({
                dataItem: this.sourceTwoCol[1],
                dataName: this.sourceTwoCol[1] + "-主键",
              });
              this.sourceTableCol.push({
                dataItem: this.sourceTwoCol[0],
                dataName: this.sourceTwoCol[0] + "-水印",
              });
              if(this.form.deriveVariableModelType === "01"){ // 四则运算
                this.getBaseVariableTestCol(this.variableArray);
              } else if(this.form.deriveVariableModelType === "02"){ // 计算引擎
                this.getBaseVariableTestCol(this.variableArray);
              } else if(this.form.deriveVariableModelType === "03"){ // 逻辑运算
                this.getBaseVariableTestCol(this.variableArray);
              }

            }

            // 添加关联字段
            setTimeout(_ => {
              this.relationField(()=>{
                this.testRunLoading = false;
              });
            },900);

            // 若测试的数据维表有字段，展示
            this.dimensionTableValueItem = true;

            // 若测试的数据源表有字段，展示
            if (this.sourceTableCol.length > 0) {
              this.sourceTableValueItem = true;
              this.form.sourceTableValue.push({})
            }
            // 给测试的结果列赋值表头    主键 和 变量英文名称
            // testResultCol
            this.testResultCol = [];
            this.testResultCol.push({
              dataName: this.sourceTwoCol[1],
              dataItem: this.sourceTwoCol[1],
            })
            this.testResultCol.push({
              dataName: this.form.variableNameEn,
              dataItem: this.form.variableNameEn
            })
          }
        })
      },

      // 点击除该区域以外的地方
      handleClickOutSide() {
        let list = document.getElementById('tableList');
        list.style.display = "none";
      },
      // 变量编辑区删除事件
      deriveVariableSqlDelete() {
        let list = document.getElementById('tableList');
        list.style.display = "none";
        let ipt = document.getElementById('ipt');
        let position = this.getPosition(ipt);
        let newStr = ipt.value.substr(0, position);
        if (this.variableArray.length > 0) {
          for (let i = 0; i < this.variableArray.length; i++) {
            // 当前坐标以某个结尾
            if (newStr.endsWith(this.variableArray[i])) {
              // 删除sql编辑区里的
              let number = this.form.deriveVariableSql.indexOf(this.variableArray[i]);
              this.form.deriveVariableSql = this.form.deriveVariableSql.substr(0, number - 1)
                + this.form.deriveVariableSql.substr(position, this.form.deriveVariableSql.length);
              for (let j = 0; j < this.variableArray.length; j++) {
                // 删除影藏区的
                this.variableArray.splice(this.variableArray.indexOf(this.variableArray[i]), 1);
                return;
              }
            }
          }
        }
      },

      deriveVariableSqlChange: function () {
        let that = this;
        let ipt = document.getElementById('ipt');
        let list = document.getElementById('tableList');
        let position = this.getPosition(ipt);
        let iStyle = window.getComputedStyle(ipt);
        let fz = parseFloat(iStyle.fontSize);
        let wd = parseFloat(iStyle.width);
        let lh = parseFloat(iStyle.lineHeight);
        let pd = parseFloat(iStyle.paddingLeft);
        let newStr = ipt.value.substr(0, position + 1);
        //有换行符根据换行符拆分
        let valArr = newStr.indexOf("\n") !== -1 ? newStr.split("\n") : [ipt.value];
        let j = 0;
        for (let i = 0; i < valArr.length; i++) {
          //把双字节的替换成两个单字节的然后再获得长度
          var len = valArr[i].replace(/[^\x00-\xff]/g, "01").length / 2;
          j += Math.ceil((len * fz) / wd);
        }
        list.style.left = (len * fz) % wd === 0 ? wd : (len * fz) % wd + pd + 10 + "px";
        list.style.top = j * lh + 25 + "px";
        list.style.display = "block";
        list.style.backgroundColor = "#f2f6fc";
        for (let i = 0; i < list.children.length; i++) {
          list.children[i].onclick = function (e) {
            ipt.value += e.target.innerHTML.substr(0, e.target.innerHTML.indexOf("【")) + "@ ";
            that.form.deriveVariableSql = ipt.value;
            list.style.display = "none";
            // 赋值
            that.variableArray = that.variableArray.concat(e.target.innerHTML.substr(0, e.target.innerHTML.indexOf("【")));
          }
        }
      },

      // 获取输入框的光标
      getPosition(element) {
        let cursorPos = 0;
        if (document.selection) {//IE
          let selectRange = document.selection.createRange();
          selectRange.moveStart('character', -element.value.length);
          cursorPos = selectRange.text.length;
        } else if (element.selectionStart || element.selectionStart === '0') {
          cursorPos = element.selectionStart;
        }
        return cursorPos;
      },

      addWhere(value) {
        let ipt = document.getElementById('ipt');
        let position = this.getPosition(ipt);
        ipt.value = ipt.value.substr(0, position)
          + value
          + ipt.value.substr(position, ipt.value.length);
      },

      // 获取基础变量
      getBaseVariableOptions(value) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/manager/list',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
        }).then(function (resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            if (value == resp.data.rows[i].variableClassification && "01" == resp.data.rows[i].variableType) {
              that.baseVariableOptions.push({
                value: resp.data.rows[i].variableNameEn,
                label: resp.data.rows[i].variableNameEn + "【" + resp.data.rows[i].variableName + "】",
                sourceInputParam: resp.data.rows[i].testSourceTableCol,
                dimensionInputParam: resp.data.rows[i].testDimensionTableCol,
              })
            }
          }
        }).catch(resp => {
          console.log('获取基础变量失败!' + resp);
        });
      },

      // 变量类型切换
      variableTypeChange(value) {
        if ('01' === value) { // 基础变量
          this.variableModelTypeItem = true;
          this.countDiv = false;
          this.processDiv = false;
          this.normalDiv = true;
          this.userDefinedDiv = false;
          this.deriveVariableDiv = false;
          this.deriveProcessModelShow = false;
          this.decisionEngineDiv = false;
          this.rules.deriveVariableSql[0].required = false;
        } else if ('02' === value) { // 派生变量
          this.variableModelTypeItem = false;
          this.countDiv = false;
          this.processDiv = false;
          this.normalDiv = false;
          this.userDefinedDiv = false;
          this.deriveVariableDiv = true;
          this.deriveProcessModelShow = false;
          this.decisionEngineDiv = true;
          this.rules.clusterName[0].required = false;
          this.rules.variableModelType[0].required = false;
          this.rules.deriveVariableSql[0].required = true;
          this.form.variableModelType = "";
          this.rules.statisticsCountModel[0].required = false;
          this.rules.variableFactor[0].required = false;
          this.normalAndCount = false;
        }
      },

      // 点击表名按钮
      tableButton(event) {
        this.form.userDefinedSql = this.form.userDefinedSql + " " + event.srcElement.innerText;
        if (this.selfQueryTestItem.length === 0) {
          this.selfQueryTestItem = this.selfQueryTestItem.concat(event.srcElement.innerText);
        } else {
          let flag = true;
          for (let i = 0; i < this.selfQueryTestItem.length; i++) {
            if (this.selfQueryTestItem[i] === event.srcElement.innerText) { // 去重判断
              flag = false;
            }
          }
          if (flag === true) {
            this.selfQueryTestItem = this.selfQueryTestItem.concat(event.srcElement.innerText);
          }
        }
      },

      redisFunctionChange(value) {
        let dom1 = document.querySelector('#redisKey');
        let dom2 = document.querySelector('#redisField');
        let dom1Item = document.querySelector('#redisKeyItem');
        let dom2Item = document.querySelector('#redisFieldItem');
        // this.form.redisKey = "";
        // this.form.redisValue = "";
        if ("01" === value) {
          this.redisKeyItem = true;
          this.redisElementDistinctItem = false;
          this.redisFieldItem = false;
          dom1.placeholder = "请选择key";
          let labelItem = dom1Item.getElementsByClassName("el-form-item__label");
          labelItem[0].innerHTML = "key";
        } else if ("02" === value) {
          this.redisKeyItem = true;
          this.redisElementDistinctItem = false;
          this.redisFieldItem = true;
          dom1.placeholder = "请选择field";
          dom2.placeholder = "请输入key";
          let label1Item = dom1Item.getElementsByClassName("el-form-item__label");
          label1Item[0].innerHTML = "field";
          let label2Item = dom2Item.getElementsByClassName("el-form-item__label");
          label2Item[0].innerHTML = "key";
        } else if ("03" === value) {
          this.redisElementDistinctItem = false;
          this.redisKeyItem = true;
          this.redisFieldItem = true;
          dom1.placeholder = "请选择value";
          dom2.placeholder = "请输入key";
          let label1Item = dom1Item.getElementsByClassName("el-form-item__label");
          label1Item[0].innerHTML = "value";
          let label2Item = dom2Item.getElementsByClassName("el-form-item__label");
          label2Item[0].innerHTML = "key";
        } else if ("04" === value) {
          this.redisElementDistinctItem = false;
          this.redisKeyItem = false;
          this.redisFieldItem = true;
          dom1.placeholder = "请输入key";
          let label2Item = dom2Item.getElementsByClassName("el-form-item__label");
          label2Item[0].innerHTML = "key";
        } else if ("05" === value) {
          this.redisElementDistinctItem = true;
          this.redisKeyItem = true;
          this.redisFieldItem = true;
          dom1.placeholder = "请选择field";
          dom2.placeholder = "请输入key";
          let label1Item = dom1Item.getElementsByClassName("el-form-item__label");
          label1Item[0].innerHTML = "field";
          let label2Item = dom2Item.getElementsByClassName("el-form-item__label");
          label2Item[0].innerHTML = "key";
        } else {
          this.redisKeyItem = false;
          this.redisFieldItem = false;
          this.redisElementDistinctItem = false;
        }
      },

      // 数据维表表字段查询
      getDimensionRelationFild(value) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
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
          console.log('获取数据源表id' + value[0] + '失败!' + resp);
        });
      },

      // 变量分类的切换
      variableClassificationChange(value, flag) {
        this.variableClassificationOptions.map((data) => {
          if (data.value === value) {
            console.log("-------【sourceTwoDabRelation】--------")
            console.log(data.sourceTwoDabRelation)
            // 数据源变量因子赋值
            if(data.sourceTwoDabRelation && data.sourceTwoDabRelation !== null && data.sourceTwoDabRelation !== ""){
              this.sourceDataChange(data.sourceDabRelation, flag);
              this.sourceDataChange(data.sourceTwoDabRelation, flag, "true");
            }
            else{
              this.sourceDataChange(data.sourceDabRelation, flag, "false");
            }
            let parse1 = JSON.parse(data.dimensionRelation);

            if (parse1 !== null && parse1.length > 0) {
              // 数据维表变量因子赋值
              this.dimensionitem = true;
              this.getDimensionRelationFild(data.dimensionRelation);
            } else {
              this.dimensionitem = false;
            }

            // 自定义表名赋值
            this.defineTableNameItem = [];
            if (parse1 !== null && parse1.length > 0) {
              this.defineDimensionFieldDiv = true;
              let tmp = "";
              for (let i = 0; i < parse1.length; i++) {
                if(parse1[i].dimensionName !== ""){
                  tmp = tmp + "_join_" + parse1[i].dimensionName;
                }
              }
              this.defineTableNameItem.push({
                value: data.sourceDabRelation + tmp,
              })
            } else {
              this.defineDimensionFieldDiv = false;
              this.defineTableNameItem.push({
                value: data.sourceDabRelation,
              });
            }

            // 自定义数据源字段赋值
            // this.defineSourceFieldItem = [];
            // let parse = JSON.parse(data.sourceFieldSchema);
            // for (let i = 0; i < parse.length; i++) {
            //   for (let key in parse[i]) {
            //     this.defineSourceFieldItem.push({
            //       value: data.sourceDabRelation + "." + key,
            //     });
            //   }
            // }
            // 基础变量选项赋值
            this.baseVariableOptions = [];
            this.getBaseVariableOptions(value);
            this.getVariableCenter(value);
            // 数据源表赋值
            this.form.sourceTableName = data.sourceDabRelation;
            this.form.dimensionId = data.dimensionRelation;
            // 清空
            this.selfQueryTestItem = [];
          }
        });
      },

      // 查询所有的变量分类
      getVariableClassification() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/classification/list',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json'
        }).then(function (resp) {
          that.variableClassificationOptions = [];
          for (let i = 0; i < resp.data.rows.length; i++) {
            // 不展示cdc
            if(resp.data.rows[i].connectorType !== '02' && resp.data.rows[i].connectorType !== '03' ){
              that.variableClassificationOptions.push({
                value: resp.data.rows[i].variableClassificationId,
                name: resp.data.rows[i].variableClassificationName,
                sourceDabRelation: resp.data.rows[i].sourceDabRelation,
                sourceTwoDabRelation: resp.data.rows[i].sourceTwoDabRelation,
                dimensionRelation: resp.data.rows[i].dimensionRelation,
                sourceFieldSchema: resp.data.rows[i].schemaDefine,
              });
            }
          }
        }).catch(resp => {
          console.log('获取变量分类请求失败：' + resp.status + ',' + resp.statusText);
        });
      },

      //运算符切换
      symbolChange(scope) {
        if (scope.row.symbol === 'between_and') {
          let dom1 = document.querySelector('#ref' + scope.$index);
          dom1.placeholder = "格式:（1,99）";
        } else if (scope.row.symbol === 'timeInterval' || scope.row.symbol === '!timeInterval') {
          let dom1 = document.querySelector('#ref' + scope.$index);
          dom1.placeholder = "时间区间规则：(hh:mm,hh:mm], 例 (00:00-24:00]";
        } else if (scope.row.symbol === 'belong' || scope.row.symbol === '!belong') {
          let dom1 = document.querySelector('#ref' + scope.$index);
          dom1.placeholder = "数字区间规则：(x,y]";
        } else {
          let dom1 = document.querySelector('#ref' + scope.$index);
          dom1.placeholder = "";
        }
      },
      // 模板类型为01时的校验
      normalCheck() {
        this.rules.clusterName[0].required = true;
        this.rules.statisticsCountModel[0].required = false;
        this.rules.variableFactor[0].required = false;
      },
      // 模板类型为统计查询时的校验
      countCheck() {
        this.rules.clusterName[0].required = false;
        this.rules.statisticsCountModel[0].required = true;
        this.rules.variableFactor[0].required = true;
      },
      // 模板类型为数据加工时的校验
      processCheck() {
        this.rules.clusterName[0].required = false;
        this.rules.statisticsCountModel[0].required = false;
        this.rules.variableFactor[0].required = false;
      },
      // 模板类型为自定义查询时的校验
      selfQueryCheck() {
        this.rules.clusterName[0].required = false;
        this.rules.statisticsCountModel[0].required = false;
        this.rules.variableFactor[0].required = false;
      },
      // 模板类型改变
      variableModelTypeChange() {
        if (this.form.variableModelType === '01') { // 普通查询
          this.normalDiv = true;
          this.countDiv = false;
          this.processDiv = false;
          this.userDefinedDiv = false;
          this.normalCheck();
          this.normalAndCount = true;
        } else if (this.form.variableModelType === '02') {  // 统计查询
          this.countDiv = true;
          this.processDiv = false;
          this.normalDiv = false;
          this.userDefinedDiv = false;
          this.countCheck();
          this.normalAndCount = true;
        } else if (this.form.variableModelType === '03') { // 数据加工
          this.processDiv = true;
          this.normalDiv = false;
          this.userDefinedDiv = false;
          this.countDiv = false;
          this.processCheck();
          this.normalAndCount = false;
        } else if (this.form.variableModelType === '04') { // 自定义查询
          this.processDiv = false;
          this.normalDiv = false;
          this.countDiv = false;
          this.userDefinedDiv = true;
          this.selfQueryCheck();
          this.normalAndCount = false;
        }
      },
      // 改变数据加工的模板，查询相对应的输入字段
      deriveProcessModelChange(val) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/funcdevelop/querySchema',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            selfFunctionId: val
          }
        }).then(function (resp) {
          if (that.form.deriveEngineTable !== undefined) {
            for (let i = 0; i < that.form.deriveEngineTable.length; i++) {
              that.form.deriveEngineTable[i].selfDataSourceOutParams = "";
            }
          }
          that.form.deriveEngineTable = resp.data.rows[0];
          let parse1 = JSON.parse(that.form.deriveInputParams);
          for (let i = 0; i < that.form.deriveEngineTable.length; i++) {
            for (let j = 0; j < parse1.length; j++) {
              if (that.form.deriveEngineTable[i].schemaDefine === parse1[j].selfFuncParam) {
                Object.assign(that.form.deriveEngineTable[i],
                  {selfDataSourceOutParams: parse1[j].outParam});
              }
            }
          }
          that.$forceUpdate();
        }).catch(resp => {
          console.log('querySelfSchema获取自定义函数的输入字段id' + val + '请求失败!' + resp);
        });
      },
      // 改变数据加工的模板，查询相对应的输入字段
      querySelfSchema(val) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/funcdevelop/querySchema',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            selfFunctionId: val
          }
        }).then(function (resp) {
          if (that.form.processTable !== undefined) {
            for (let i = 0; i < that.form.processTable.length; i++) {
              that.form.processTable[i].selfDataSourceOutParams = "";
            }
          }
          that.form.processTable = resp.data.rows[0];
          that.form.processOutputTable = resp.data.rows[1];
          for (let i = 0; i < that.processModelOptions.length; i++) {
            if (val === that.processModelOptions[i].value) {
              that.form.selfFunctionNameCn = that.processModelOptions[i].selfFunctionNameCn;
            }
          }
          that.$forceUpdate();
        }).catch(resp => {
          console.log('querySelfSchema获取自定义函数的输入字段id' + val + '请求失败!' + resp);
        });
      },
      // 改变数据加工的模板，查询相对应的输入字段
      querySelfSchemaUpdate(val) {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/variable/funcdevelop/querySchema',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json',
          params: {
            selfFunctionId: val
          }
        }).then(function (resp) {
          that.form.processTable = resp.data.rows[0];
          that.form.processOutputTable = resp.data.rows[1];
          for (let i = 0; i < that.processModelOptions.length; i++) {
            if (val === that.processModelOptions[i].value) {
              that.form.selfFunctionNameCn = that.processModelOptions[i].selfFunctionNameCn;
            }
          }
          let parse = JSON.parse(that.form.processInputParams);
          for (let i = 0; i < that.form.processTable.length; i++) {
            Object.assign(that.form.processTable[i],
              {selfDataSourceOutParams: parse[i].outParam,
                  functionType: parse[i].functionType,
                  beginIndex: parse[i].beginIndex,
                  lengthNum: parse[i].lengthNum
                });
          }
          that.$forceUpdate();
        }).catch(resp => {
          console.log('querySelfSchemaUpdate获取自定义函数的输入字段id' + val + '请求失败!' + resp);
        });
      },
      // 统计查询的条件编辑
      leftKuoHao() {
        this.form.statisticsConditions = this.form.statisticsConditions + ' ( ';
      },
      rightKuoHao() {
        this.form.statisticsConditions = this.form.statisticsConditions + ' ) ';
      },
      andButton() {
        this.form.statisticsConditions = this.form.statisticsConditions + ' and ';
      },
      orButton() {
        this.form.statisticsConditions = this.form.statisticsConditions + ' or ';
      },
      deleteLast() {
        if (this.form.statisticsConditions.length > 0) {
          this.form.statisticsConditions = this.form.statisticsConditions.substr(0, this.form.statisticsConditions.length - 2);
        }
      },
      clearContext() {
        this.form.statisticsConditions = "";
      },
      // 选择该行数据加入下面文本
      joinContext(scope) {
        if (scope.row.field === "" || scope.row.symbol === "") {
          this.$alert("条件项的字段名称和运算符不能为空！", {type: "error"});
          return false;
        }
        let field =  scope.row.field;
        if (scope.row.functionType !== "") {
          if (scope.row.functionType === "SUBSTRING") {
            field = scope.row.functionType + "(" + scope.row.field + "," + scope.row.beginIndex + "," + scope.row.lengthNum + ")";
          } else {
            field = scope.row.functionType + "(" + scope.row.field + ")";
          }
        }
        if (scope.row.symbol === 'between_and') {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            'between_and(' + field + ",'" + scope.row.value + "')";
        } else if (scope.row.symbol === 'contain') {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            'contain(' + field + ",'" + scope.row.value + "')";
        } else if (scope.row.symbol === 'not_contain') {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            'not_contain(' + field + ",'" + scope.row.value + "')";
        } else if (scope.row.symbol === 'starts_with') {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            field + ".starts_with('" + scope.row.value + "')";
        } else if (scope.row.symbol === 'ends_with') {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            field + ".ends_with('" + scope.row.value + "')";
        } else {
          this.form.statisticsConditions = this.form.statisticsConditions + " " +
            field + scope.row.symbol + scope.row.value
        }
      },
      // 删除指定列
      deleteConditionTable(scope) {
        this.form.conditionTable.splice(scope.$index, 1);
      },
      // 增加一行统计条件选项
      addConditionTable() {
        this.form.conditionTable.push({
          field: '',
          type:'',
          functionType:'',
          beginIndex:'',
          lengthNum:'',
          symbol: '',
          value: ''
        });
      },

      // 查询计算模板
      getVariableModelType() {
        listFunction(this.queryParams).then(resp => {
          for (let i = 0; i < resp.rows.length; i++) {
            if (resp.rows[i].functionType === "01"){
              this.processModelOptions.push({
                value: resp.rows[i].selfFunctionId,
                name: resp.rows[i].selfFunctionNameCn,
                selfFunctionNameCn: resp.rows[i].functionName,
              })
            }
            if (resp.rows[i].functionType === "03"){
              this.decisionModelOptions.push({
                value: resp.rows[i].selfFunctionId,
                name: resp.rows[i].selfFunctionNameCn,
                selfFunctionNameCn: resp.rows[i].functionName,
              })
            }
          }
          this.$forceUpdate();
        }).catch(resp => {
          console.log('获取数据加工计算模板请求失败!');
        });
      },

      // 切换变量分类
      sourceDataChange(value, flag, valueTwo) {
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
            console.log("-----------------【resp】--------------------")
            console.log(resp);
            // 多次点击先清空。否则会重复叠加
            if (flag !== "test") {
              that.form.variableFactor = "";
            }
            if(!valueTwo || valueTwo === "false"){
              if(valueTwo === "false"){
                that.dataSourceTwoName = "";
                that.variableFactorTwoOptions = [];
                that.statisticsGroupTwoOptions = [];
                that.statisticsfieldTwoOptions = [];
              }
              that.dataSourceName = "数据源表："+value;
              that.variableFactorOptions = [];
              that.statisticsGroupOptions = [];
              that.statisticsfieldOptions = [];
              for (let j = 0; j < resp.data.rows[0].length; j++) {
                that.variableFactorOptions.push({
                  label: value + "." + resp.data.rows[0][j].key, // 表名.字段
                  value: value + "." + resp.data.rows[0][j].key,
                  type: resp.data.rows[0][j].value,// 类型
                });
                // 给统计分组辅助
                that.statisticsGroupOptions.push({
                  name: value + "." + resp.data.rows[0][j].key,
                  value: value + "." + resp.data.rows[0][j].key
                });
                // 给统计分组辅助
                that.statisticsfieldOptions.push({
                  name: value + "." + resp.data.rows[0][j].key,
                  value: value + "." + resp.data.rows[0][j].key,
                  type: resp.data.rows[0][j].value,
                });
              }
              that.sourceTwoCol = [];
              for (let i = 0; i < resp.data.rows[1].length; i++) {
                that.sourceTwoCol = that.sourceTwoCol.concat(resp.data.rows[1][i]);
              }
            }
            else if(valueTwo === "true"){
              that.dataSourceTwoName = "数据源表："+value;
              that.variableFactorTwoOptions = [];
              that.statisticsGroupTwoOptions = [];
              that.statisticsfieldTwoOptions = [];
              for (let j = 0; j < resp.data.rows[0].length; j++) {
                that.variableFactorTwoOptions.push({
                  label: value + "." + resp.data.rows[0][j].key, // 表名.字段
                  value: value + "." + resp.data.rows[0][j].key,
                  type: resp.data.rows[0][j].value,// 类型
                });
                // 给统计分组辅助
                that.statisticsGroupTwoOptions.push({
                  name: value + "." + resp.data.rows[0][j].key,
                  value: value + "." + resp.data.rows[0][j].key
                });
                // 给统计分组辅助
                that.statisticsfieldTwoOptions.push({
                  name: value + "." + resp.data.rows[0][j].key,
                  value: value + "." + resp.data.rows[0][j].key,
                  type: resp.data.rows[0][j].value,
                });
              }
              // that.sourceTwoCol = [];
              // for (let i = 0; i < resp.data.rows[1].length; i++) {
              //   that.sourceTwoCol = that.sourceTwoCol.concat(resp.data.rows[1][i]);
              // }
            }


          }).catch(resp => {
            console.log('获取数据源表id' + val + '请求失败!' + resp);
          });

        }, 300);
      },

      // 切换数据维表
      dimensionDataChange(val) {
        if (val.length === 1) {
          let that = this;
          const baseUrl = process.env.VUE_APP_BASE_API;
          axios({
            method: 'get',
            url: baseUrl + '/system/Dimension/tableNameQuery',
            headers: {'Authorization': 'Bearer ' + getToken()},
            responseType: 'json',
            params: {
              connType: val
            }
          }).then(function (resp) {
            for (let i = 0; i < that.dimensionDataOptions.length; i++) {
              if (that.dimensionDataOptions[i].value === val[0]) {
                // 多次点击先清空。否则会重复叠加
                that.dimensionDataOptions[i].dimensionSchemas = [];
                for (let j = 0; j < resp.data.rows.length; j++) {
                  // 只有JDBC的时候才有第三级
                  if (resp.data.rows[j].connectorType === '02') {
                    that.dimensionDataOptions[i].dimensionSchemas.push({
                      label: resp.data.rows[j].clusterName,
                      value: resp.data.rows[j].clusterName,
                      dimensionSchemas: []
                    });
                  } else {
                    that.dimensionDataOptions[i].dimensionSchemas.push({
                      label: resp.data.rows[j].clusterName,
                      value: resp.data.rows[j].clusterName
                    });
                  }
                }
              }
            }
          }).catch(resp => {
            console.log('获取数据源表id' + val[0] + '失败!' + resp);
          });

        } else if (val.length === 2) {
          // 切换数据维表
          setTimeout(_ => {
            let that = this;
            const baseUrl = process.env.VUE_APP_BASE_API;
            axios({
              method: 'get',
              url: baseUrl + '/system/Dimension/querySchema',
              headers: {'Authorization': 'Bearer ' + getToken()},
              responseType: 'json',
              params: {
                dimensionId: val[1]
              }
            }).then(function (resp) {
              // 第一级val为 50,60,70  坐标0,1,2
              for (let j = 0; j < that.dimensionDataOptions.length; j++) {
                if (that.dimensionDataOptions[j].value === val[0]) {
                  for (let k = 0; k < that.dimensionDataOptions[j].dimensionSchemas.length; k++) {
                    if (that.dimensionDataOptions[j].dimensionSchemas[k].value === val[1]) {
                      that.dimensionDataOptions[j].dimensionSchemas[k].dimensionSchemas = [];
                      for (let i = 0; i < resp.data.rows.length; i++) {
                        that.dimensionDataOptions[j].dimensionSchemas[k].dimensionSchemas.push({
                          label: resp.data.rows[i],
                          value: resp.data.rows[i]
                        })
                      }
                    }
                  }
                }
              }
            }).catch(resp => {
              console.log('获取数据维表id' + val[1] + '失败!' + resp);
            });
          }, 300);
        }
      },
      getVersionList(data) {
        this.loading = true;
        this.queryParams.variableName = data.variableName;
        listVersionCenter(this.queryParams).then(response => {
          this.versionList = response.rows;
          this.loading = false;
          this.queryParams.variableName = undefined;
        });
      },
      getOperateList(data) {
        this.loading = true;
        this.queryParams.variableNameEn = data.variableNameEn;
        listOperate(this.queryParams).then(response => {
          this.operateList = response.rows;
          this.loading = false;
          this.queryParams.variableNameEn = undefined;
        });
      },

      /** 查询变量管理中心列表 */
      getList() {
        this.loading = true;
        listCenter(this.queryParams).then(response => {
          this.centerList = response.rows;
          this.total = response.total;
          this.loading = false;
        });
      },
      // 变量类型 01基础变量 02 派生变量字典翻译
      variableTypeFormat(row) {
        return this.selectDictLabel(this.variableTypeOptions, row.variableType);
      },
      // 返回按钮
      backButton() {
        this.addDiv = false;
        this.layoutOne = true;
        this.detailDiv = false;
      },
      // 取消按钮
      cancel() {
        this.addDiv = false;
        this.layoutOne = true;
        this.detailDiv = false;
        this.reset();
      },
      // 表单重置
      reset() {
        this.form = {
          variableId: undefined,
          variableName: undefined,
          variableNameEn: undefined,
          variableType: "01",
          deriveVariableModelType: "01",
          description: undefined,
          deriveProcessModel: undefined,
          variableModelType: "01",
          clusterName: undefined,
          statisticsCountModel: undefined,
          statisticsModel: undefined,
          statisticsNum: undefined,
          globalVariableStartTime: undefined,
          globalVariableStopTime: undefined,
          statisticsCycle: undefined,
          statisticsConditions: "",
          statisticsConditionOption: "",
          createTime: undefined,
          modifyTime: undefined,
          conditionTable: [],
          processModel: undefined,
          processInputParams: undefined,
          deriveInputParams: undefined,
          watermark: undefined,
          dataSourcename: undefined,
          versionNum: undefined,
          createBy: undefined,
          statisticsSelfFunctionItem: {
            effectFunction:undefined,
            beginIndex:undefined,
            lengthNum:undefined,
          },
          variableClassification: undefined,
          variableFactor: undefined,
          sourceTableName: undefined,
          dimensionId: undefined,
          redisFunction: undefined,
          redisElementDistinct: undefined,
          redisKey: undefined,
          redisSelfFunctionItem: {
            effectFunction:undefined,
            beginIndex:undefined,
            lengthNum:undefined,
          },
          redisValue: undefined,
          selfFunctionNameCn: undefined,
          deriveBaseVariable: [],
          statisticsGroupItem: [
            {
              groupField:"",
              groupFunction:"",
              beginIndex:"",
              lengthNum:"",
            }
          ],
          userDefinedSql: "",
          deriveVariableSql: "",
          // 数据源表输入字段
          sourceTableValue: [],
          // 数据源表主键
          sourceKey: undefined,
          // 数据源表的类型
          variableFactorType: undefined,
          // 测试的数据源表字段
          testSourceTableCol: [],
          // 测试的数据源维表字段
          testDimensionTableCol: undefined,

          testDimdata: undefined,
        };
        this.resetForm("form");
        this.processModelOptions = [];
        this.decisionModelOptions = [];
        this.normalDiv = true;
        this.countDiv = false;
        this.processDiv = false;
        this.userDefinedDiv = false;
        this.deriveVariableDiv = false;
        this.deriveProcessModelShow = false;
        this.decisionEngineDiv = false;
        this.variableModelTypeItem = true;
        this.sourceDataOptions = [];
        this.variableFactorOptions = [];
        this.variableClassificationOptions = [];
        this.listDimension = [];
        this.dimensionDataOptions = [];
        this.dimensionitem = false;
        this.open = false;
        this.variableArray = [];
        this.baseVariableOptions = [];
        this.selfQueryTestItem = [];
        this.allVariableOptions = [];
        this.sourceTableCol = [];
        this.sourceTwoCol = [];
        this.redisSelfFunctionOptions = [];
        this.normalAndCount = true;
        this.redisSelfFunctionShow = false;
        this.statisticsSelfFunctionShow = false;
        this.statisticsModelShow = true;
        this.globalVariableModelShow = false;
        this.deriveVariableSqlShow = true;
        this.testRunButton = false;
        this.getVariableClassification();

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
        this.ids = selection.map(item => item.variableId);
        let nameTmp = selection.map(item => item.createBy);
        this.names = new Set(nameTmp);
        this.single = selection.length !== 1;
        this.multiple = !selection.length
      },
      /** 新增按钮操作 */
      handleAdd() {
        // 跳转页面
        //直接跳转
        this.addDiv = true;
        this.submitButton = true;
        this.layoutOne = false;
        this.versionNumShow = false;
        this.detailViem = false;
        this.UnAllowedUpdate = false;
        this.testButton = false;
        // 查询数据源表
        this.getSourceData();
        // 查询数据源维表
        this.getDimensionData();
        // 查询自定义模板表
        this.getVariableModelType();
        // 查询所有的作用函数
        this.getFunctionTypeOptions();
        this.redisSelfFunctionChange("");
        // 第一次点击【新增】的时候
        this.rules.statisticsCountModel[0].required = false;
        this.rules.variableFactor[0].required = false;
        this.rules.deriveVariableSql[0].required = false;

      },
      // 查询数据源表
      getSourceData() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/source/manage/list',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json'
        }).then(function (resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            that.sourceDataOptions.push({
              value: resp.data.rows[i].dataSourceId,
              label: resp.data.rows[i].tableName,
              watermark: resp.data.rows[i].waterMarkName,
            });
          }
        }).catch(resp => {
          console.log('获取数据源表请求失败getSourceData：' + resp.status + ',' + resp.statusText);
        });
      },
      // 查询数据维表
      getDimensionData() {
        let that = this;
        const baseUrl = process.env.VUE_APP_BASE_API;
        axios({
          method: 'get',
          url: baseUrl + '/system/Dimension/tableNameQuery',
          headers: {'Authorization': 'Bearer ' + getToken()},
          responseType: 'json'
        }).then(function (resp) {
          for (let i = 0; i < resp.data.rows.length; i++) {
            that.dimensionDataOptions.push({
              value: resp.data.rows[i].clusterName,
              label: resp.data.rows[i].clusterName,
            });
          }
        }).catch(resp => {
          console.log('获取数据源表请求失败getDimensionData：' + resp.status + ',' + resp.statusText);
        });
      },
      // 查看变量的版本详细
      versionDetail(row){
        this.detailDiv = true;
        this.layoutOne = false;
        this.getVersionList(row);
        this.getOperateList(row);
      },



      /** 详情按钮操作 */
      handleDetail(row) {
        this.detailViem = true;
        this.UnAllowedUpdate = true;
        this.versionNumShow = true;
        this.updateLoading = true;
        this.addDiv = true;
        this.submitButton = false;
        this.layoutOne = false;
        this.detailDiv = false;
        this.reset();
        const variableId = row.variableId || this.ids;
        getCenter(variableId).then(response => {
          this.form = response.data;
          this.form.conditionTable = [];
          this.form.conditionTable = JSON.parse(this.form.statisticsConditionOption);
          this.form.statisticsGroupItem = JSON.parse(this.form.statisticsGroupItem);
          this.form.redisSelfFunctionItem = JSON.parse(this.form.redisSelfFunctionItem);
          this.form.statisticsSelfFunctionItem = JSON.parse(this.form.statisticsSelfFunctionItem);
          this.redisSelfFunctionChange(this.form.redisSelfFunctionItem.effectFunction);
          this.statisticsSelfFunctionChange(this.form.statisticsSelfFunctionItem.effectFunction);

          // 查询所有的作用函数
          this.getFunctionTypeOptions();
          // 变量类型
          this.variableTypeChange(this.form.variableType);
          // 模板类型
          this.variableModelTypeChange();
          // redis方法
          this.redisFunctionChange(this.form.redisFunction);
          // 查询数据源维表
          this.getDimensionData();
          //变量分类的切换
          this.variableClassificationChange(this.form.variableClassification, "test");
          // 查询自定义模板表
          this.getVariableModelType();

          // 派生变量模板类型
          this.deriveVariableModelTypeChange(this.form.deriveVariableModelType);
          // 决策模板输入项
          if(this.form.deriveProcessModel !== null){
            this.deriveProcessModelChange(this.form.deriveProcessModel);
          }
          if (this.form.processModel != null) {
            this.querySelfSchemaUpdate(this.form.processModel);
          }
          this.statisticsModelChange(this.form.statisticsModel);
        }).then(res => {
          this.updateLoading = false;
        })
        setTimeout(()=>{
          for (let i = 0; i < this.form.statisticsGroupItem.length; i++) {
            this.groupFunctionChange(this.form.statisticsGroupItem[i].groupFunction,i);
          }
          for (let i = 0; i < this.form.conditionTable.length; i++) {
            this.conditionTableFunctionChangeDetail(this.form.conditionTable[i].functionType,i);
          }
        },600);

      },

      // 测试变量按钮
      testVariabel(row){
        this.testButton = true;
        this.detailViem = true;
        this.addDiv = true;
        this.submitButton = false;
        this.layoutOne = false;
        this.detailDiv = false;
        this.testAndUpdate(row);
      },

      /** 修改按钮操作 */
      handleUpdate(row) {
        // 只有自己能修改
        if (row.createBy !== this.name){
          this.$message.error("该数据是"+row.createBy+"创建的，您不能修改！");
          return false;
        }

        this.detailViem = false;
        this.submitButton = true;
        this.testButton = false;
        this.testAndUpdate(row);

      },
      // 测试和修改通用代码
      testAndUpdate(row){
        this.updateLoading = true;
        this.UnAllowedUpdate = true;
        this.versionNumShow = true;
        this.reset();

        const variableId = row.variableId || this.ids;
        getCenter(variableId).then(response => {
          this.form = response.data;
          this.variableTmp = response.data;
          this.form.conditionTable = [];
          this.form.sourceTableValue = [];
          this.form.processTable = [];

          this.form.conditionTable = JSON.parse(this.form.statisticsConditionOption);
          this.form.statisticsGroupItem = JSON.parse(this.form.statisticsGroupItem);
          this.form.redisSelfFunctionItem = JSON.parse(this.form.redisSelfFunctionItem);
          this.form.statisticsSelfFunctionItem = JSON.parse(this.form.statisticsSelfFunctionItem);
          this.redisSelfFunctionChange(this.form.redisSelfFunctionItem.effectFunction);
          this.statisticsSelfFunctionChange(this.form.statisticsSelfFunctionItem.effectFunction);// 分组 作用函数切换
          this.statisticsModelChange(this.form.statisticsModel);

          // 派生变量的返显
          if ("02" === this.form.variableType) {
            let parse = JSON.parse(this.form.deriveBaseVariable);
            this.variableArray = [];
            for (let i = 0; i < parse.length; i++) {
              this.variableArray = this.variableArray.concat(parse[i]);
            }

            console.log("--1-1-1");
            console.log(this.variableArray);
          }
          // 查询所有的作用函数
          this.getFunctionTypeOptions();
          // 变量类型
          this.variableTypeChange(this.form.variableType);
          // 模板类型
          this.variableModelTypeChange();
          // 查询数据源维表
          this.getDimensionData();
          // redis方法
          this.redisFunctionChange(this.form.redisFunction);
          // 查询自定义模板表 和 决策模板
          this.getVariableModelType();



          // 派生变量模板类型
          this.deriveVariableModelTypeChange(this.form.deriveVariableModelType);
          // 决策模板输入项
          if(this.form.deriveProcessModel !== null){
            this.deriveProcessModelChange(this.form.deriveProcessModel);
          }

          //变量分类的切换
          setTimeout(()=>{
            this.variableClassificationChange(this.form.variableClassification, "test");
          },600);
          setTimeout(()=>{
            if (this.form.processModel != null){
              this.querySelfSchemaUpdate(this.form.processModel)
            }
          },600);
        }).then(res => {
          this.updateLoading = false;
        });
        setTimeout(()=>{
          if(this.form.statisticsGroupItem !== null){
            for (let i = 0; i < this.form.statisticsGroupItem.length; i++) {
              this.groupFunctionChange(this.form.statisticsGroupItem[i].groupFunction,i);
            }
          }
          if(this.form.conditionTable !== null){
            for (let i = 0; i < this.form.conditionTable.length; i++) {
              this.conditionTableFunctionChangeDetail(this.form.conditionTable[i].functionType,i);
            }
          }
          if (this.form.processTable !== null) {
            for (let i = 0; i < this.form.processTable.length; i++) {
              this.selfFunctionChnage(this.form.processTable[i].functionType, i);
            }
          }
        },2000);
        this.addDiv = true;
        this.layoutOne = false;
      },


      /** 提交按钮 */
      submitForm() {
        this.$refs["form"].validate((valid) => {
            if (valid) {
              if (this.form.variableId !== undefined) {
                 if (this.form.variableType === '01') {  // 基础变量
                  this.setSourceTableCol();
                  if (this.form.variableModelType === '01') { // 普通查询
                    // this.form.clusterName = this.form.clusterName.toString();
                    this.form.statisticsConditionOption = JSON.stringify(this.form.conditionTable);
                  } else if (this.form.variableModelType === '02') { // 统计查询
                    this.form.statisticsConditionOption = JSON.stringify(this.form.conditionTable);
                    // 若是数据源表的
                    this.variableFactorOptions.map((data) => {
                      if (data.label === this.form.variableFactor) {
                        this.form.variableFactorType = data.type;
                      }
                    });
                    // 若数据维表的
                    for (let i = 0; i < this.listDimension.length; i++) {
                      this.listDimension[i].dimensionRelationOptions.map((data) => {
                        if (data.key === this.form.variableFactor) {
                          this.form.variableFactorType = data.type;
                        }
                      })
                    }
                  } else if (this.form.variableModelType === '03') { // 数据加工
                    let arr = [];
                    for (let i = 0; i < this.form.processTable.length; i++) {
                      arr.push({
                        selfFuncParam: this.form.processTable[i].schemaDefine,
                        outParam: this.form.processTable[i].selfDataSourceOutParams,
                        beginIndex: this.form.processTable[i].beginIndex,
                        lengthNum: this.form.processTable[i].lengthNum,
                        functionType: this.form.processTable[i].functionType,
                      })
                    }
                    this.form.processInputParams = JSON.stringify(arr);
                  }
                } else if (this.form.variableType === '02') {
                   if("01" === this.form.deriveVariableModelType){  // 四则运算
                     this.getBaseVariableTestCol(this.variableArray);
                   } else if("02" === this.form.deriveVariableModelType){ // 计算引擎
                     // 保存计算引擎输入参数
                     let arr = [];
                     // [{"selfFuncParam":"source1","outParam":"1112"},{"selfFuncParam":"source2","outParam":"222"},{"selfFuncParam":"sourceMap","outParam":["ad","e"]}]
                     this.variableArray = [];
                     for (let i = 0; i < this.form.deriveEngineTable.length; i++) {
                       arr.push({
                         selfFuncParam: this.form.deriveEngineTable[i].schemaDefine,
                         outParam: this.form.deriveEngineTable[i].selfDataSourceOutParams,
                         type: this.form.deriveEngineTable[i].dataBaseType,
                       });
                       if(this.form.deriveEngineTable[i].dataBaseType === "map"){
                         for (let j = 0; j < this.form.deriveEngineTable[i].selfDataSourceOutParams.length; j++) {
                           this.variableArray = this.variableArray.concat(this.form.deriveEngineTable[i].selfDataSourceOutParams[j]);
                         }
                       }
                     }
                     this.form.deriveInputParams = JSON.stringify(arr);
                     this.getBaseVariableTestCol(this.variableArray);
                   }else if("03" === this.form.deriveVariableModelType){ // 逻辑运算
                     this.getBaseVariableTestCol(this.variableArray);
                   }

                }
                // 保存测试的数据源表与维表字段
                setTimeout(() => {
                  this.saveTestCol();
                  // 主键赋值
                  this.form.deriveBaseVariable = JSON.stringify(this.variableArray);
                  this.form.sourceKey = this.sourceTwoCol[1];
                  this.form.watermark = this.sourceTwoCol[0];
                  updateCenter(this.form).then(response => {
                    if (response.code === 200) {
                      this.msgSuccess("修改成功");
                      this.addDiv = false;
                      this.layoutOne = true;
                      this.reset();
                      this.getList();
                    } else {
                      this.msgError(response.msg);
                    }
                  });
                },1000);


              } else {
                if (this.form.variableType === '01') {  // 基础变量
                  this.setSourceTableCol();
                  if (this.form.variableModelType === '01') { // 普通查询
                    this.form.statisticsConditionOption = JSON.stringify(this.form.conditionTable);
                  } else if (this.form.variableModelType === '02') { // 统计查询
                    this.form.statisticsConditionOption = JSON.stringify(this.form.conditionTable);
                    // 若是数据源表的
                    this.variableFactorOptions.map((data) => {
                      if (data.label === this.form.variableFactor) {
                        this.form.variableFactorType = data.type;
                      }
                    });
                    // 若数据维表的
                    for (let i = 0; i < this.listDimension.length; i++) {
                      this.listDimension[i].dimensionRelationOptions.map((data) => {
                        if (data.key === this.form.variableFactor) {
                          this.form.variableFactorType = data.type;
                        }
                      })
                    }
                  } else if (this.form.variableModelType === '03') { // 数据加工
                    let arr = [];
                    for (let i = 0; i < this.form.processTable.length; i++) {
                      arr.push({
                        selfFuncParam: this.form.processTable[i].schemaDefine,
                        outParam: this.form.processTable[i].selfDataSourceOutParams,
                        functionType: this.form.processTable[i].functionType,
                        beginIndex: this.form.processTable[i].beginIndex,
                        lengthNum: this.form.processTable[i].lengthNum,
                      })
                    }
                    this.form.processInputParams = JSON.stringify(arr);
                  }
                  // 基础变量时，派生变量的模板类型清空
                  this.form.deriveVariableModelType = "";
                } else if (this.form.variableType === '02') {
                  if("01" === this.form.deriveVariableModelType){
                    this.getBaseVariableTestCol(this.variableArray);
                  } else if("02" === this.form.deriveVariableModelType){
                    // 保存计算引擎输入参数
                    let arr = [];
                    this.variableArray = [];
                    for (let i = 0; i < this.form.deriveEngineTable.length; i++) {
                      arr.push({
                        selfFuncParam: this.form.deriveEngineTable[i].schemaDefine,
                        outParam: this.form.deriveEngineTable[i].selfDataSourceOutParams,
                        type: this.form.deriveEngineTable[i].dataBaseType,
                      });
                      if(this.form.deriveEngineTable[i].dataBaseType === "map"){
                        for (let j = 0; j < this.form.deriveEngineTable[i].selfDataSourceOutParams.length; j++) {
                          this.variableArray = this.variableArray.concat(this.form.deriveEngineTable[i].selfDataSourceOutParams[j]);
                        }
                      }
                    }
                    this.form.deriveInputParams = JSON.stringify(arr);
                    this.getBaseVariableTestCol(this.variableArray);
                  } else if("03" === this.form.deriveVariableModelType){
                    this.getBaseVariableTestCol(this.variableArray);
                  }
                }
                setTimeout(() => {
                  this.saveTestCol();
                  this.form.deriveBaseVariable = JSON.stringify(this.variableArray);
                  // 主键赋值
                  this.form.sourceKey = this.sourceTwoCol[1];
                  this.form.watermark = this.sourceTwoCol[0];
                  addCenter(this.form).then(response => {
                    if (response.code === 200) {
                      this.msgSuccess("新增成功");
                      this.addDiv = false;
                      this.layoutOne = true;
                      this.reset();
                      this.getList();
                    } else {
                      this.msgError(response.msg);
                    }
                  });
                },1000);
              }
            }
          }
        );
      },

      // 赋值数据维表的测试字段
      setDimensionTableCol(){
        this.baseVariableOptions.map((data) => {
          for (let i = 0; i < this.variableArray.length; i++) {
            if (data.value === this.variableArray[i]) {
              let parse = JSON.parse(data.sourceInputParam);
              if (parse != null) {
                for (let j = 0; j < parse.length; j++) {
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
              let parseDiemsion = JSON.parse(data.dimensionInputParam);
              if (parseDiemsion != null) {
                for (let j = 0; j < parseDiemsion.length; j++) {
                  for (let l = 0; l < parseDiemsion[j].col; l++) {
                    // 都没有。最后才增加一个。  如果有。去重则不增肌。否则增肌在那个里面
                    let flag1 = true; // 如果有。去重则不增肌。否则增肌在那个里面
                    let flag2 = true; // 都没有。最后才增加一个
                    let num = "";
                    group:for (let m = 0; m < this.listResultDimension.length; m++) {
                      if (parseDiemsion[j].name === this.listResultDimension[m].name) { // 同一个维表 this.listResultDimension增加 dimensionTableCol
                        flag2 = false;
                        for (let k = 0; k < this.listResultDimension[m].dimensionTableCol.length; k++) {
                          if (this.listResultDimension[m].dimensionTableCol[k].dataItem === parseDiemsion[j].col[l]) {   // 去重
                            flag1 = false;
                            break group;
                          }

                        }
                        num = m;
                        break group;
                      }
                    }
                    // 新增数据维表
                    if (flag2 === true) {
                      flag1 = false;
                      this.listResultDimension.push({
                        name: parseDiemsion[j].name,
                      })
                      let tmp = [];
                      for (let k = 0; k < parseDiemsion[j].col; k++) {
                        tmp.push({
                          dataItem: parseDiemsion[j].col[k],
                          dataName: parseDiemsion[j].col[k],
                        })
                      }

                      let dmo = [];
                      dmo.push({});
                      if (this.listResultDimension.length === 0) {
                        Object.assign(this.listResultDimension[0], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[0], {dimensionTableValue: dmo}); // 增加输入框
                      } else {
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableCol: tmp}); // 不是第一个时
                        Object.assign(this.listResultDimension[this.listResultDimension.length - 1], {dimensionTableValue: dmo}); // 增加输入框
                      }
                    }
                    // 有数据维表 增加一个列
                    if (flag1 === true) {
                      this.listResultDimension[num].dimensionTableCol.push({
                        dataItem: parseDiemsion[j].col[l],
                        dataName: parseDiemsion[j].col[l],
                      })
                    }
                  }
                }
              }
            }
          }
        });
      },
      // 保存测试的数据源表与维表字段
      saveTestCol(){
        // 把sql所用参数保存，便于派生变量测试时调用
        this.form.testSourceTableCol = [];
        let s = [];
        if (this.sourceTableCol.length > 0) {
          for (let i = 0; i < this.sourceTableCol.length; i++) {
            s = s.concat(this.sourceTableCol[i].dataName);
          }
        }
        this.form.testSourceTableCol = JSON.stringify(s);
        // 保存数据维表的  格式： 表名+字段
        // [{"name":"数据维表:jdbc","col":["id","iphone"]},{"name":"数据维表:hbase","col":["列族1.列名1","列族2.列名2"]}]
        this.form.testDimensionTableCol = [];
        let t = [];
        for (let j = 0; j < this.listResultDimension.length; j++) {
          if (this.listResultDimension[j].dimensionTableCol.length > 0) {
            t.push({
              name: this.listResultDimension[j].name,
            });
            let m = [];
            for (let i = 0; i < this.listResultDimension[j].dimensionTableCol.length; i++) {
              m.push(this.listResultDimension[j].dimensionTableCol[i].dataName)
            }
            Object.assign(t[t.length-1],{col:m});
          }
        }
        this.form.testDimensionTableCol = JSON.stringify(t);
      },

      /** 删除按钮操作 */
      handleDelete(row) {

        // 只有自己能删除
        if (row.variableId !== undefined) {
          if (row.createBy !== this.name) {
            this.$message.error("该数据为" + row.createBy + "创建，您不能删除！");
            return false;
          }
        } else {
          if (this.names.size > 1 || !this.names.has(this.name)) {
            this.$message.error("您只能删除自己创建的数据");
            return false;
          }
        }

        const variableIds = row.variableId || this.ids;
        this.$confirm('是否确认删除变量管理中心编号为"' + variableIds + '"的数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return delCenter(variableIds);
        }).then(() => {
          this.getList();
          this.msgSuccess("删除成功");
        }).catch(function () {
        });
      },
      /** 导出按钮操作 */
      handleExport() {
        const queryParams = this.queryParams;
        this.$confirm('是否确认导出所有变量管理中心数据项?', "警告", {
          confirmButtonText: "确定",
          cancelButtonText: "取消",
          type: "warning"
        }).then(function () {
          return exportCenter(queryParams);
        }).then(response => {
          this.download(response.msg);
        }).catch(function () {
        });
      }
    }
  };
</script>
<style>
  /*  下拉框分组的样式 */
  .el-select-group__title {
    padding-left: 20px;
    font-size: 14px;
    color: #606266;
    line-height: 30px;
    font-weight: bold;
  }

  #tableList {
    position: absolute;
    list-style: none;
    width: 500px;
    height: 200px;
    display: none;
    overflow-x: scroll;
    overflow-y: scroll;
  }

  .ulClass {
    display: block;
    list-style-type: disc;
    margin-block-start: 1em;
    margin-block-end: 1em;
    margin-inline-start: 0px;
    margin-inline-end: 0px;
    padding-inline-start: 10px;
    border: 1px solid #000000;
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

  .customWidth{
    width:80%;
  }
</style>
