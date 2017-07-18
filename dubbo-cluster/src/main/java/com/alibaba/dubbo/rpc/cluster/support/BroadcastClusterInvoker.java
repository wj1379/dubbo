/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.cluster.support;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

/**
 * BroadcastClusterInvoker
 *
 * @author william.liangf
 */
public class BroadcastClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(BroadcastClusterInvoker.class);

    public BroadcastClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        RpcContext.getContext().setInvokers((List)invokers);
        RpcException exception = null;
        Result result = null;

        /**
         * by wangjun21
         * 如果是发布/订阅模式，订阅方又是以集群形式部署的情况下，只需要成功调用集群中一台Provider即可。这里是用Application名称区分集群部署的应用。
         */
        Set<String> invokedApps = new HashSet<String>(invokers.size());
        for (Invoker<T> invoker: invokers) {
            try {
                String providerApplication = getProviderApplication(invoker);
                if(providerApplication != null && invokedApps.contains(providerApplication)){
                    logger.info("providerApplication " + providerApplication + " has already been successfully invoked, ignore this time.");
                    continue;
                }

                result = invoker.invoke(invocation);
                if(providerApplication != null){
                    invokedApps.add(providerApplication);
                }
            } catch (RpcException e) {
                exception = e;
                logger.warn(e.getMessage(), e);
            } catch (Throwable e) {
                exception = new RpcException(e.getMessage(), e);
                logger.warn(e.getMessage(), e);
            }
        }
        if (exception != null) {
            throw exception;
        }
        return result;
    }

    private String getProviderApplication(Invoker<T> invoker) {
        if("com.alibaba.dubbo.registry.integration.RegistryDirectory$InvokerDelegete"
                .equals(invoker.getClass().getName())){
            try {
                Method method = invoker.getClass().getMethod("getProviderUrl", new Class[]{});
                URL url = (URL)method.invoke(invoker, new Object[]{});
                return url.getParameter("application");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}