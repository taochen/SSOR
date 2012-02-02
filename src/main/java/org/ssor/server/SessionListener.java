package org.ssor.server;

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.http.HttpSessionBindingListener;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import org.ssor.CollectionFacade;
import org.ssor.server.fake.FakeSession;

@SuppressWarnings("unchecked")
public class SessionListener implements ServletContextListener,
		ServletRequestListener, HttpSessionBindingListener,
		HttpSessionListener, HttpSessionAttributeListener {

	private long timeout = 60000;

	private final static Map<String, HttpSession> sessions = CollectionFacade
			.getConcurrentHashMap();

	ServletContext sc;

	public static HttpSession getSession(String id) {
		FakeSession session = (FakeSession)sessions.get(id);
		if (session == null)
			sessions.put(id, session = new FakeSession(null));
		else
			session.resetActivation();
		return session;
	}

	@Override
	public void sessionCreated(HttpSessionEvent event) {
		sessions.put(event.getSession().getId(), new FakeSession(event
				.getSession()));

	}

	@Override
	public void sessionDestroyed(HttpSessionEvent event) {
		sessions.remove(event.getSession().getId());

	}

	@Override
	public void attributeAdded(HttpSessionBindingEvent se) {

	}

	@Override
	public void attributeRemoved(HttpSessionBindingEvent se) {
		// TODO Auto-generated method stub

	}

	@Override
	public void attributeReplaced(HttpSessionBindingEvent se) {
		// TODO Auto-generated method stub

	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		// TODO Auto-generated method stub

	}

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		// TODO Auto-generated method stub
		final String timeoutString = sce.getServletContext().getInitParameter(
				"session-timeout");
		// Default is 20 mins
		if(timeoutString == null)
			timeout = 20 * 6000;
		else
			timeout = Integer.parseInt(timeoutString) * 6000;
		// The quartz timer
		new Timer().schedule(new TimerTask() {

			public void run() {

				final Set<Map.Entry<String, HttpSession>> set = sessions.entrySet();
				for(Map.Entry<String, HttpSession> entry : set){
					if(((FakeSession)entry.getValue()).isCanDeleted(timeout))
						sessions.remove(entry.getKey());
				}
			}

		}, 10, timeout - 6000);
	}

	@Override
	public void requestDestroyed(ServletRequestEvent sre) {
		// TODO Auto-generated method stub

	}

	@Override
	public void requestInitialized(ServletRequestEvent sre) {
		// TODO Auto-generated method stub

	}

	@Override
	public void valueBound(HttpSessionBindingEvent event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void valueUnbound(HttpSessionBindingEvent event) {
		// TODO Auto-generated method stub

	}

}
