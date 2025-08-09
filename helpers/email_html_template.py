def html_template(
    current_tier,
    old_tier,
    customer_name,
    customer_email,
    sum_amount=0,
    transaction_count=0,
):

    dashboard_url = "https://www.youtube.com"
    cta_color = ""

    unsubscribe_url = "https://www.google.com"
    level = {
        "Platinum": {
            "header_color": "#D9D9D9",
            "new_tier_color": "#8e44ad",
            "grade": 1,
        },
        "Gold": {"header_color": "#f39c12", "new_tier_color": "#f39c12", "grade": 2},
        "Silver": {"header_color": "#95a5a6", "new_tier_color": "#95a5a6", "grade": 3},
        "Bronze": {"header_color": "#d68910", "new_tier_color": "#d68910", "grade": 4},
    }

    arrow_symbol = "&#8594;"

    if current_tier.lower().strip() == old_tier.lower().strip():
        notification_type = "Tier"
        main_message = "<p>We're truly grateful for your continued trust and support. Your loyalty inspires us to keep delivering the best products and service possible. Here's to many more moments together!\n\nThank you for being part of our journey.</p>"
        arrow_color = "#000000"
        next_tier_info = ""
    elif level[current_tier]["grade"] < level[old_tier]["grade"]:
        notification_type = "Tier Upgrade"
        main_message = f"<p>Congratulations! You've been upgraded from {old_tier} to {current_tier}.\n\nEnjoy your new perks and rewards—you've earned them!</p>"
        arrow_color = "#008000"
    else:
        notification_type = "Tier Downgrade"
        main_message = "<p>Your tier has changed from Gold to Silver.\n\nBut don't worry, you're just a few steps away from regaining Gold and unlocking all its perks again. We can't wait to celebrate your return!</p>"
        arrow_color = "#FA5053"

    # Tier upgrade message
    if current_tier == "Gold":
        next_tier_info = f"""
        <div style="margin: 0 0 30px 0; padding: 20px; background-color: #D9D9D9; border-left: 4px solid #B5B5B5; border-radius: 4px;">
            <h4 style="margin: 0 0 10px 0; color: #636363;">Next Goal: Platinum Tier</h4>
            <p style="margin: 0; color: #636363; font-size: 14px;">
                Spend ₱{(100000-sum_amount):,.2f} more and complete {20-transaction_count} more transactions to unlock Platinum benefits!
            </p>
        </div>
        """

        tier_benefits = """
        <li>Priority Support — faster ticket resolution times.</li>
        <li>Early Feature Previews — access upcoming tools before general release.</li>
        <li>Quarterly Usage Review — recommendations to maximize subscription value.</li>
        <li>Moderate Usage Boost — higher quotas compared to Silver/Bronze.</li>
        <li>Exclusive Webinars & Training — expert-led sessions for power users.</li>
        """

    elif current_tier == "Silver":
        next_tier_info = f"""
        <div style="margin: 0 0 30px 0; padding: 20px; background-color: #fff3cd; border-left: 4px solid #ffc107; border-radius: 4px;">
            <h4 style="margin: 0 0 10px 0; color: #856404;">Next Goal: Gold Tier</h4>
            <p style="margin: 0; color: #856404; font-size: 14px;">
                Spend ₱{(30000-sum_amount):,.2f} more and complete {10-transaction_count} more transactions to unlock Gold benefits!
            </p>
        </div>
        """

        tier_benefits = """
        <li>Extended Support Hours — support beyond standard business hours.</li>
        <li>Invites to Select Webinars — product best-practices and tips.</li>
        <li>One-Time Setup Optimization — review to improve account efficiency.</li>
        <li>Basic Usage Boost — slightly increased quotas vs. Bronze.</li>
        <li>Discounted Add-ons — reduced rates on select premium features.</li>
        """
    elif current_tier == "Bronze":
        next_tier_info = f"""
        <div style="margin: 0 0 30px 0; padding: 20px; background-color: #E0E0E0; border-left: 4px solid #ffc107; border-radius: 4px;">
            <h4 style="margin: 0 0 10px 0; color: #666666;">Next Goal: Silver Tier</h4>
            <p style="margin: 0; color: #666666; font-size: 14px;">
                Spend ₱{(20000-sum_amount):,.2f} more and complete {5-transaction_count} more transactions to unlock Silver benefits!
            </p>
        </div>
        """

        tier_benefits = """
        <li>Extended Support Hours — support beyond standard business hours.</li>
        <li>Invites to Select Webinars — product best-practices and tips.</li>
        <li>One-Time Setup Optimization — review to improve account efficiency.</li>
        <li>Basic Usage Boost — slightly increased quotas vs. Bronze.</li>
        <li>Discounted Add-ons — reduced rates on select premium features.</li>
        """
    else:
        next_tier_info = ""
        tier_benefits = """
        <li>Dedicated Account Manager — direct support & priority handling.</li>
        <li>Exclusive Beta Access — try new features before public release.</li>
        <li>Free Quarterly Strategy Consultation — one-on-one business optimization.</li>
        <li>Higher Usage Limits — expanded quotas on reports, API calls, and seats.</li>
        <li>Loyalty Perks — annual gift pack or account credits.</li>
        """

    template = f"""
    <!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tier Update Notification</title>
</head>
<body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f4f4f4;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background-color: #f4f4f4;">
        <tr>
            <td style="padding: 20px 0;">
                <!-- Main container -->
                <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="600" style="margin: 0 auto; background-color: #ffffff; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">

                    <!-- Header -->
                    <tr>
                        <td style="padding: 30px 40px 20px 40px; text-align: center; background-color: {level[current_tier]['header_color']}; border-radius: 8px 8px 0 0;">
                            <h1 style="margin: 0; color: #ffffff; font-size: 24px; font-weight: bold;">
                                {notification_type} Notification
                            </h1>
                        </td>
                    </tr>

                    <!-- Main content -->
                    <tr>
                        <td style="padding: 40px;">

                            <!-- Greeting -->
                            <p style="margin: 0 0 20px 0; font-size: 16px; color: #333333; line-height: 1.5;">
                                Dear {customer_name},
                            </p>

                            <!-- Main message -->
                            <p style="margin: 0 0 30px 0; font-size: 16px; color: #333333; line-height: 1.6;">
                                {main_message}
                            </p>

                            <!-- Tier change visual -->
                            <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="margin: 0 0 30px 0;">
                                <tr>
                                    <td style="text-align: center; padding: 20px; background-color: #f8f9fa; border-radius: 6px;">
                                        <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="margin: 0 auto;">
                                            <tr>
                                                <td style="text-align: center; padding: 0 15px;">
                                                    <div style="background-color: {level[old_tier]['header_color']}; color: #ffffff; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: bold; margin-bottom: 5px;">
                                                        {old_tier}
                                                    </div>
                                                    <div style="font-size: 12px; color: #666666;">Previous</div>
                                                </td>
                                                <td style="padding: 0 20px; font-size: 30px; color: {arrow_color};">
                                                    <span>{arrow_symbol}</span>
                                                </td>
                                                <td style="text-align: center; padding: 0 15px;">
                                                    <div style="background-color: {level[current_tier]['new_tier_color']}; color: #ffffff; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: bold; margin-bottom: 5px;">
                                                        {current_tier}
                                                    </div>
                                                    <div style="font-size: 12px; color: #666666;">Current</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>

                            <!-- Current stats -->
                            <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="margin: 0 0 30px 0;">
                                <tr>
                                    <td style="padding: 20px; background-color: #f8f9fa; border-radius: 6px;">
                                        <h3 style="margin: 0 0 15px 0; color: #333333; font-size: 18px;">Your Current Status</h3>
                                        <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                                            <tr>
                                                <td style="padding: 8px 0; border-bottom: 1px solid #e9ecef;">
                                                    <strong style="color: #333333;">Total Spent:</strong>
                                                    <span style="color: #666666; float: right;">₱{sum_amount:,}</span>
                                                </td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px 0;">
                                                    <strong style="color: #333333;">Transaction Count:</strong>
                                                    <span style="color: #666666; float: right;">{transaction_count}</span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>

                            <!-- Next tier info (if applicable) -->
                            {next_tier_info}

                            <!-- Benefits -->
                            <div style="margin: 0 0 30px 0;">
                                <h3 style="margin: 0 0 15px 0; color: #333333; font-size: 18px;">Your {current_tier} Benefits</h3>
                                <ul style="margin: 0; padding-left: 20px; color: #666666; line-height: 1.6;">
                                    {tier_benefits}
                                </ul>
                            </div>

                            <!-- Enable when user dashboard is available -->
                            <!-- Call to action -->
                            <!-- <div style="text-align: center; margin: 0 0 20px 0;">
                                <a href="{dashboard_url}" style="display: inline-block; background-color: {cta_color}; color: #ffffff; text-decoration: none; padding: 12px 30px; border-radius: 25px; font-weight: bold; font-size: 16px;">
                                    View My Account
                                </a>
                            </div> -->

                            <!-- Closing -->
                            <p style="margin: 0; font-size: 16px; color: #333333; line-height: 1.5;">
                                Thank you for your continued loyalty!
                            </p>

                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="padding: 20px 40px; background-color: #f8f9fa; border-radius: 0 0 8px 8px; text-align: center;">
                            <p style="margin: 0; font-size: 12px; color: #666666; line-height: 1.4;">
                                This email was sent to {customer_email}.<br>
                                If you have any questions, please contact our support team.<br>
                                <a href="{unsubscribe_url}" style="color: #666666;">Unsubscribe</a>
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    """

    return template
